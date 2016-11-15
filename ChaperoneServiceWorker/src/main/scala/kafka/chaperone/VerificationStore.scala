/*
 * Copyright (c) 2016 Uber Technologies, Inc. (streaming-core@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.chaperone

import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.{EventFactory, EventHandler, EventTranslatorThreeArg}
import com.yammer.metrics.core.Gauge
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.Logging

/**
 * VerificationStore encapsulate the transaction details. And it has a background thread
 * to scan the database for unsent AuditMsg and submit them to AuditMsgReporter to resend.
 * The 'mark-as-sent' operations are batched to be done in async to avoid blocking operation
 * in critical data path.
 *
 * VerificationStore uses zk to store offsets to share them among ChaperoneServices instances in
 * case topic/partition is re-distributed by HelixController.
 *
 * VerificationStore uses a searchable store (like MySQl or HSQLDB) locally as WAL for sending in case
 * to resend AuditMsg failing to send out.
 */
class VerificationStore(val zkOffsetStore: ZkOffsetStore,
                        val auditMsgStore: AuditMsgStore,
                        val auditMsgReporter: AuditMsgReporter,
                        val auditMsgRetentionMs: Long,
                        val resendLocalBatchSize: Int,
                        val resendLocalDelayInSec: Int,
                        val resendLocalIntervalInSec: Int,
                        val resendZkDelayInMs: Int,
                        val resendZkIntervalInMs: Int,
                        val queueSize: Int,
                        val hostMeta: HostMetadata,
                        val requireExactlyOnce: Boolean) extends Logging with KafkaMetricsGroup {
  // executor uses LinkedBlockingQueue
  private val executor = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setNameFormat("vs-marksent-executor-%d").build)

  // scheduled-executor uses heap
  private val cronExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder().setNameFormat("vs-cron-executor-%d").build)

  private val markTaskSubmitted = newMeter("markTaskSubmittedPerSec", "vstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val oldRecordRemoved = newMeter("oldRecordRemovedPerSec", "vstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val oldRecordFromLocalStoreResend = newMeter("oldRecordFromLocalStoreResendPerSec", "vstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val oldRecordFromZkStoreResend = newMeter("oldRecordFromZkStoreResendPerSec", "vstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))

  private val removeOldTimer = new KafkaTimer(newTimer("removeOldTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer")))
  private val resendOldFromLocalStoreTimer = new KafkaTimer(newTimer("resendOldFromLocalStoreTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer")))
  private val resendOldFromZkStoreTimer = new KafkaTimer(newTimer("resendOldFromZkStoreTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer")))
  private val markTaskTimer = new KafkaTimer(newTimer("markTaskTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer")))
  private val zkOffsetStoreTimer = new KafkaTimer(newTimer("zkOffsetStoreTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer")))
  private val auditMsgStoreTimer = new KafkaTimer(newTimer("auditMsgStoreTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer")))

  // do resend before the watermark to avoid mixture of new AuditMsg and old AuditMsg
  private val unsentBufferForLocalStore = new util.LinkedList[LocalAuditMsg]
  private val unsentBufferForZkStore = new util.LinkedList[LocalAuditMsg]

  private var allOldAuditMsgInLocalIsProcessed = false

  // asynchronize the I/O
  private val storeExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("verification-store-%d").build)
  private val disruptor = new Disruptor[TaskEntry](new TaskEntryFactory, queueSize, storeExecutor)
  private val ringBuffer = disruptor.getRingBuffer
  disruptor.handleEventsWith(new TaskEntryHandler(this))

  private val storeQueueSize = newGauge("storeRemainingQueueSize", new Gauge[Long] {
    def value = disruptor.getRingBuffer.remainingCapacity()
  }, Map("metricsType" -> "gauge"))


  private val TRANSLATOR: EventTranslatorThreeArg[TaskEntry, String, util.HashMap[Int, Long], util.HashMap[Double, TimeBucketMetadata]] =
    new EventTranslatorThreeArg[TaskEntry, String, util.HashMap[Int, Long], util.HashMap[Double, TimeBucketMetadata]] {
      override def translateTo(entry: TaskEntry, sequence: Long, topicName: String, offsets: util.HashMap[Int, Long],
                               buckets: util.HashMap[Double, TimeBucketMetadata]) {
        entry.topicName = topicName
        entry.offsets = offsets
        entry.buckets = buckets
      }
    }


  def close() {
    info("Stopping verification store disruptor...")
    disruptor.shutdown()
    storeExecutor.shutdownNow

    zkOffsetStore.close()

    info("Stop marksent executor...")
    executor.shutdownNow

    info("Stop cron executor...")
    cronExecutor.shutdownNow

    auditMsgStore.close()
  }

  def open() {
    info("Starting verification store disruptor...")
    disruptor.start

    zkOffsetStore.open()
    auditMsgStore.open()

    val selfVerificationStore = this

    info("Resend unsent AuditMsg in zk store before watermarkMs=" + zkOffsetStore.currentEpochInMs)
    cronExecutor.scheduleWithFixedDelay(new Runnable {
      override def run() {
        if (zkOffsetStore.getOldTopicCount() > 0) {
          info("Start to resend old records in zk once again...")
          resendOldFromZkStoreTimer.time {
            unsentBufferForZkStore.clear()
            zkOffsetStore.getUnsentAuditMsg(unsentBufferForZkStore)

            if (unsentBufferForZkStore.size() > 0) {
              val topicName = unsentBufferForZkStore.getFirst.topicName

              // remove AuditMsg that's already sent according to local store
              val itr = unsentBufferForZkStore.iterator()
              while (itr.hasNext) {
                val auditMsg = itr.next()
                if (auditMsgStore.checkSentAuditMsg(auditMsg.uuid)) {
                  info("AuditMsg for topicName=%s with UUID=%s has been sent. Skip".format(auditMsg.topicName, auditMsg.uuid))
                  itr.remove()
                }
              }
              if (unsentBufferForZkStore.size() > 0) {
                auditMsgReporter.reportAuditMsg(async = false, unsentBufferForZkStore, selfVerificationStore)
                oldRecordFromZkStoreResend.mark(unsentBufferForZkStore.size())
              }

              zkOffsetStore.removeUnsentAuditMsg(topicName)
            }
          }
          info("ZkStore Resend done")

          if (zkOffsetStore.getOldTopicCount() == 0) {
            warn("All old AuditMsgs in zk store has been processed. Task goes idle")
          }
        }
      }
    }, resendZkDelayInMs, resendZkIntervalInMs, TimeUnit.MILLISECONDS)

    info("Resend unsent AuditMsg in local store before watermarkMs=" + auditMsgStore.currentEpochInMs)
    cronExecutor.scheduleWithFixedDelay(new Runnable {
      override def run() {
        if (!allOldAuditMsgInLocalIsProcessed) {
          info("Start to resend old records in local store once again...")
          resendOldFromLocalStoreTimer.time {
            unsentBufferForLocalStore.clear()
            auditMsgStore.getUnsentAuditMsg(unsentBufferForLocalStore, resendLocalBatchSize)

            if (unsentBufferForLocalStore.size() > 0) {
              auditMsgReporter.reportAuditMsg(async = false, unsentBufferForLocalStore, selfVerificationStore)
              oldRecordFromLocalStoreResend.mark(unsentBufferForLocalStore.size())
            }
          }
          info("LocalStore Resend done")

          if (unsentBufferForLocalStore.size() == 0) {
            warn("All old AuditMsgs in local store has been processed. Task goes idle")
            allOldAuditMsgInLocalIsProcessed = true
          }
        }
      }
    }, resendLocalDelayInSec, resendLocalIntervalInSec, TimeUnit.SECONDS)

    info("Setup old AuditMsg cleanup task with retentionMs=" + auditMsgRetentionMs)
    cronExecutor.scheduleWithFixedDelay(new Runnable {
      override def run() {
        info("Start to remove old records once again...")
        removeOldTimer.time {
          val ts = System.currentTimeMillis - auditMsgRetentionMs
          val removed = auditMsgStore.removeOldRecord(ts)
          oldRecordRemoved.mark(removed)
        }
        info("Remove done")
      }
    }, 1, 15, TimeUnit.MINUTES)
  }

  /**
   * In-async to reduce the blocking impact to KafkaProducer's thread
   */
  def markAsSent(uuid: String) {
    markTaskSubmitted.mark()

    executor.submit(new Runnable {
      override def run() {
        markTaskTimer.time {
          auditMsgStore.markAuditMsg(uuid)
        }
      }
    })
  }

  def convertToLocalAuditMsg(topicName: String, buckets: util.HashMap[Double, TimeBucketMetadata]): util.LinkedList[LocalAuditMsg] = {
    val ret = new util.LinkedList[LocalAuditMsg]
    val itr = buckets.values().iterator()
    while (itr.hasNext) {
      val bucket = itr.next()
      ret.add(bucket.toLocalAuditMsg(hostMeta.asIdentity + "_" + LocalAuditMsg.nextSuffix(), topicName, hostMeta))
    }
    ret
  }


  /**
   * Has to be in-sync before KafkaProducer sending to acts as WAL
   */
  def store(topicName: String, offsets: util.HashMap[Int, Long], buckets: util.HashMap[Double, TimeBucketMetadata]): Unit = {
    ringBuffer.publishEvent(TRANSLATOR, topicName, offsets, buckets)
  }

  private def doStore(topicName: String, offsets: util.HashMap[Int, Long], buckets: util.HashMap[Double, TimeBucketMetadata]) {
    val auditMsgs = convertToLocalAuditMsg(topicName, buckets)
    debug("Converted into LocalAuditMsg=" + auditMsgs)

    if (requireExactlyOnce) {
      zkOffsetStoreTimer.time {
        zkOffsetStore.store(topicName, offsets, auditMsgs)
      }

      auditMsgStoreTimer.time {
        auditMsgStore.store(topicName, auditMsgs)
      }
    }

    auditMsgReporter.reportAuditMsg(async = true, auditMsgs, this)
  }

  // helper classes for Disruptor
  private class TaskEntry(var topicName: String, var offsets: util.HashMap[Int, Long], var buckets: util.HashMap[Double, TimeBucketMetadata])

  private class TaskEntryFactory extends EventFactory[TaskEntry] {
    override def newInstance(): TaskEntry = new TaskEntry(null, null, null)
  }

  private class TaskEntryHandler(val verificationStore: VerificationStore) extends EventHandler[TaskEntry] {
    override def onEvent(entry: TaskEntry, sequence: Long, endOfBatch: Boolean) {
      verificationStore.doStore(entry.topicName, entry.offsets, entry.buckets)
      entry.topicName = null
      entry.offsets = null
      entry.buckets = null
    }
  }

}
