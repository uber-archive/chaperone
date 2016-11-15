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
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.collect.Sets
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.{EventFactory, EventHandler, EventTranslatorOneArg}
import com.yammer.metrics.core.Gauge
import kafka.message.MessageAndMetadata
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.Logging

/**
 * MessageAuditor is a single-threaded module. It decodes, audits msg and submit
 * AuditMsg to AuditMsgReporter when thresholds are reached
 *
 * In particular, MessageAuditor uses a set of MessageAggregator to audit the whitelisted topics.
 * Each MessageAggregator handles one topic. All MessageAuditors share a single AuditMsgReporter,
 * which publishes the AuditMsg to kafka after storing the AuditMsg in Persistent Queue.
 */
class MessageAuditor(val auditorId: Int,
                     val timestampExtractor: ITimestampExtractor,
                     val queueSize: Int,
                     val initialMapSize: Int,
                     val timeBucketIntervalInSec: Long,
                     val reportFreqBucketCount: Int,
                     val reportFreqIntervalInMs: Long,
                     val verificationStore: VerificationStore) extends Logging with KafkaMetricsGroup {
  private val auditExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
    "message-auditor-" + auditorId + "-%d").build)

  private val disruptor = new Disruptor[MessageEntry](new MessageEntryFactory, queueSize, auditExecutor)
  private val ringBuffer = disruptor.getRingBuffer
  disruptor.handleEventsWith(new MessageEntryHandler(this))

  private val aggregators = new util.HashMap[String /*topicName*/ , MessageAggregator](initialMapSize)
  private val topicsServed = Sets.newConcurrentHashSet[String]()

  // 999999999999 (12 digits), as millisecond, means Sun Sep 09 2001 01:46:39 UTC.
  // Thus, all today's time, as in ms, is larger than this number. Besides,
  // 1000000000000 (13 digits), as second, means Fri Sep 27 33658 01:46:40....
  private val MIN_TIMESTAMP_MS = 999999999999L

  private val TRANSLATOR: EventTranslatorOneArg[MessageEntry, MessageAndMetadata[Array[Byte], Array[Byte]]] =
    new EventTranslatorOneArg[MessageEntry, MessageAndMetadata[Array[Byte], Array[Byte]]]() {
      override def translateTo(entry: MessageEntry, sequence: Long, msg: MessageAndMetadata[Array[Byte], Array[Byte]]) {
        entry.msg = msg
      }
    }

  private val auditorQueueSize = newGauge("auditorRemainingQueueSize", new Gauge[Long] {
    def value = disruptor.getRingBuffer.remainingCapacity()
  }, Map("auditorId" -> auditorId.toString, "metricsType" -> "gauge"))

  private val msgToAudit = newMeter("messageToAuditPerSec", "audit", TimeUnit.SECONDS,
    Map("auditorId" -> auditorId.toString, "metricsType" -> "meter"))
  private val auditMsgGenerated = newMeter("auditMsgGeneratedPerSec", "audit", TimeUnit.SECONDS,
    Map("auditorId" -> auditorId.toString, "metricsType" -> "meter"))
  private val auditMsgSendTimer = new KafkaTimer(newTimer("auditMsgSendTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS,
    Map("auditorId" -> auditorId.toString, "metricsType" -> "timer")))
  private val msgFailedToAudit = newMeter("messageFailedToAuditPerSec", "audit", TimeUnit.SECONDS,
    Map("auditorId" -> auditorId.toString, "metricsType" -> "meter"))

  private val timestampExtractTimer = new KafkaTimer(newTimer("timestampExtractTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS,
    Map("auditorId" -> auditorId.toString, "metricsType" -> "timer")))

  private val triggerMsgTimer = new KafkaTimer(newTimer("triggerMsgTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS,
    Map("auditorId" -> auditorId.toString, "metricsType" -> "timer")))

  private val cronExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(
    "message-auditor-cron-" + auditorId + "-%d").build)

  def start() {
    info("Starting message auditor[%d]...".format(auditorId))
    disruptor.start
    cronExecutor.scheduleWithFixedDelay(new Runnable {
      override def run() {
        info("Auditor[%d] starts to submit triggerMsg once again...".format(auditorId))
        triggerToCheckThreshold()
        info("Auditor[%d] finished triggering".format(auditorId))
      }
    }, timeBucketIntervalInSec, timeBucketIntervalInSec, TimeUnit.SECONDS)
  }

  def shutdown() {
    info("Stopping message auditor=[%d]...".format(auditorId))
    cronExecutor.shutdownNow()
    disruptor.shutdown()
    auditExecutor.shutdown()
  }

  def submitMessage(msg: MessageAndMetadata[Array[Byte], Array[Byte]]) {
    ringBuffer.publishEvent(TRANSLATOR, msg)
  }

  private def getTimestamp(topicName: String, msg: MessageAndMetadata[Array[Byte], Array[Byte]],
                           aggregator: MessageAggregator): Double = {
    timestampExtractTimer.time {
      try {
        val ts = timestampExtractor.getTimestamp(msg.message())
        if (ts < 0) {
          aggregator.markNoTimeMsg(1)
          debug("Auditor[%d] got message from topic=%s without timestamp".format(auditorId, topicName))
        } else {
          return ts
        }
      } catch {
        case e: Exception =>
          aggregator.markMalformedMsg(1)
          debug("Auditor[%d] failed to get ts from message for topicName=%s, partition=%d, offset=%d with exception=%s".format(
            auditorId, topicName, msg.partition, msg.offset, e.getMessage))
      }
      System.currentTimeMillis() / 1000.0
    }
  }

  private def triggerToCheckThreshold() {
    try {
      triggerMsgTimer.time {
        val itr = topicsServed.iterator()
        while (itr.hasNext) {
          val topicName = itr.next()
          val triggerMsg = new MessageAndMetadata[Array[Byte], Array[Byte]](topicName, -1, null, -1, null, null)
          submitMessage(triggerMsg)
        }
      }
    }
    catch {
      case e: Exception =>
        warn("Auditor[%d] failed to submit triggerMsg with exception=%s".format(auditorId, e.getMessage))
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t
    }
  }

  private def isTriggerMsg(msg: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
    msg.partition == -1 && msg.offset == -1
  }

  private def getMsgSize(msg: MessageAndMetadata[Array[Byte], Array[Byte]]): Long = {
    var size = 0
    val key = msg.key()
    val payload = msg.message()
    if (key != null) {
      size += key.length
    }
    if (payload != null) {
      size += payload.length
    }
    size
  }

  private def auditMessage(msg: MessageAndMetadata[Array[Byte], Array[Byte]]) {
    msgToAudit.mark()

    val topicName = msg.topic

    var aggregator = aggregators.get(topicName)

    if (aggregator == null) {
      info("Auditor[%d] initializing message tracker for topic=%s".format(auditorId, topicName))

      aggregator = new MessageAggregator(topicName, timeBucketIntervalInSec, reportFreqBucketCount, reportFreqIntervalInMs)
      aggregators.put(topicName, aggregator)

      topicsServed.add(topicName)
    }

    if (aggregator != null) {
      var timestamp = 0.0
      try {
        var readyToReportAuditMsg = false

        if (!isTriggerMsg(msg)) {
          timestamp = getTimestamp(topicName, msg, aggregator)

          debug("Auditor[%d] got timestamp=%f from msg of topic=%s".format(auditorId, timestamp, topicName))

          // change millisecond to second for consistency
          if (timestamp > MIN_TIMESTAMP_MS) {
            timestamp /= 1000
          }

          readyToReportAuditMsg = aggregator.track(msg.partition, msg.offset, timestamp, 1, getMsgSize(msg))

          debug("Auditor[%d] tracked msg with topic=%s, partition=%d, offset=%d, timestamp=%f".format(
            auditorId, topicName, msg.partition, msg.offset, timestamp))
        }
        else {
          readyToReportAuditMsg = aggregator.isTimeoutToReport
          info("Auditor[%d] got triggerMsg for topic=%s, auditMsgIsTriggered=%s".format(
            auditorId, topicName, readyToReportAuditMsg))
        }

        if (readyToReportAuditMsg) {
          val buckets = aggregator.getAndResetBuffer()
          val offsets = aggregator.getAndResetOffsets()

          auditMsgGenerated.mark(buckets.size())

          info("Auditor[%d] Reporting buckets count=%d for topic=%s, offsets=%s".format(auditorId, buckets.size(), topicName, offsets))
          auditMsgSendTimer.time {
            verificationStore.store(topicName, offsets, buckets)
          }
        }
      }
      catch {
        case e: Exception =>
          msgFailedToAudit.mark()
          warn("Auditor[%d] failed to track message, topicName=%s, timestamp=%f".format(auditorId, topicName, timestamp), e)
      }
    }
  }

  // helper classes for Disruptor
  private class MessageEntry(var msg: MessageAndMetadata[Array[Byte], Array[Byte]])

  private class MessageEntryFactory extends EventFactory[MessageEntry] {
    override def newInstance(): MessageEntry = new MessageEntry(null)
  }

  private class MessageEntryHandler(val messageAuditor: MessageAuditor) extends EventHandler[MessageEntry] {
    override def onEvent(entry: MessageEntry, sequence: Long, endOfBatch: Boolean) {
      messageAuditor.auditMessage(entry.msg)
      entry.msg = null
    }
  }

}
