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
import java.util.concurrent.TimeUnit

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.mirrormaker.MirrorMakerWorker
import kafka.utils.Logging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/**
 * AuditMsgReporter is a funtional thread-safe component.
 * It stores AuditMsg and Offsets into VerificationStore in transaction;
 * and sends msg to kafka via KafkaProducer
 */
class AuditMsgReporter(val hostMeta: HostMetadata,
                       val auditTopicName: String,
                       val kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]]) extends Logging with KafkaMetricsGroup {
  private val msgEncodeTimer = new KafkaTimer(newTimer("msgEncodeTimer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer")))
  private val msgFailedToEncode = newMeter("msgFailedToEncodePerSec", "report", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val msgFailedToSendOut = newMeter("msgFailedToSendOutPerSec", "report", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val msgSendTimer = newTimer("msgSendTimer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("metricsType" -> "timer"))

  def reportAuditMsg(async: Boolean, localAuditMsgs: util.LinkedList[LocalAuditMsg], verificationStore: VerificationStore) {
    if (MirrorMakerWorker.isShuttingDown()) {
      warn("The MirrorMakerThread is shutting down. Stop to report AuditMsg to kafka.")
      return
    }

    if (async) {
      debug("Reporting AuditMsgs count=%d to Kafka in async".format(localAuditMsgs.size()))
    } else {
      debug("Reporting AuditMsgs count=%d to Kafka in sync".format(localAuditMsgs.size()))
    }

    val localAuditMsgItr = localAuditMsgs.iterator()
    while (localAuditMsgItr.hasNext) {
      if (MirrorMakerWorker.isShuttingDown()) {
        warn("The MirrorMakerThread is shutting down. Stop to report AuditMsg to kafka.")
        return
      }

      val localAuditMsg = localAuditMsgItr.next()

      val auditMsg = encodeAuditMsg(localAuditMsg)

      if (auditMsg != null) {
        if (async) {
          sendOutAsync(localAuditMsg, auditMsg, verificationStore)
        } else {
          sendOutSync(localAuditMsg, auditMsg, verificationStore)
        }
      }
    }
  }

  def sendOutAsync(localAuditMsg: LocalAuditMsg, auditMsg: Array[Byte], verificationStore: VerificationStore) {
    debug("Sending AuditMsg=%s to topic=%s for topic=%s in async".format(localAuditMsg, auditTopicName, localAuditMsg.topicName))

    val uuid: String = localAuditMsg.uuid
    val timerCtx = msgSendTimer.time()
    kafkaProducer.send(new ProducerRecord[Array[Byte], Array[Byte]](auditTopicName, null /*key*/ , auditMsg),
      new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          try {
            if (exception == null) {
              verificationStore.markAsSent(uuid)
            } else {
              msgFailedToSendOut.mark()
              error("Got exception to send out AuditMsg=%s to topic=%s in async for topic=%s".format(
                localAuditMsg, auditTopicName, localAuditMsg.topicName), exception)
              MirrorMakerWorker.cleanShutdown()
            }
          }
          finally {
            if (timerCtx != null) {
              timerCtx.stop()
            }
          }
        }
      })
  }

  def sendOutSync(localAuditMsg: LocalAuditMsg, auditMsg: Array[Byte], verificationStore: VerificationStore): Unit = {
    debug("Sending AuditMsg=%s to topic=%s for topic=%s in sync".format(localAuditMsg, auditTopicName, localAuditMsg.topicName))

    val uuid: String = localAuditMsg.uuid
    val timerCtx = msgSendTimer.time()
    try {
      kafkaProducer.send(new ProducerRecord[Array[Byte], Array[Byte]](auditTopicName, null /*key*/ , auditMsg)).get()
      verificationStore.markAsSent(uuid)
    }
    catch {
      case e: Exception =>
        msgFailedToSendOut.mark()
        error("Got exception to send out AuditMsg=%s to topic=%s in sync for topic=%s".format(
          localAuditMsg, auditTopicName, localAuditMsg.topicName), e)
        MirrorMakerWorker.cleanShutdown()
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t;
    }
    finally {
      timerCtx.stop()
    }
  }

  def encodeAuditMsg(localAuditMsg: LocalAuditMsg): Array[Byte] = {
    var auditMsg: Array[Byte] = null
    try {
      msgEncodeTimer.time {
        auditMsg = AuditMsgGenerator.fromAuditMsgRecord(localAuditMsg)
      }
    }
    catch {
      case e: Exception =>
        msgFailedToEncode.mark()
        warn("Unable to encode local AuditMsg for topic=" + localAuditMsg.topicName, e)
        // TODO: skip it for now
        auditMsg = null
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t;
    }
    auditMsg
  }
}
