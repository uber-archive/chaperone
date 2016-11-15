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

import com.alibaba.fastjson.{JSON, JSONArray}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, ZKGroupDirs, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.apache.zookeeper.Op

/**
 * To cooperate with local store to achieve transactional semantics, the AuditMsgs
 * associated with the offsets to commit are also stored in zookeeper 'atomically'.
 * This ensures no AuditMsg is lost during shutdown. AuditMsgs in zk are resent on restart.
 *
 * Another tricky issue is how to ensure every topic/partition is processed by only
 * one consumer at any time, so that the offsets in zk are consistent to every worker.
 * In particular, HelixController issues requests to workers in parallel. To handle this,
 * we currently slow start restarted Chaperone. The ultimate solution is make HelixController
 * issue requests to worker serially.
 */
class ZkOffsetStore(val currentEpochInMs: Long,
                    val zkServers: String,
                    val zkConnectionTimeoutMs: Int,
                    val zkSessionTimeoutMs: Int,
                    val zkGroupId: String,
                    val hostMeta: HostMetadata) extends Logging with KafkaMetricsGroup {
  private var zkClient: ZkClient = null
  private var zkUtils: ZkUtils = null

  private val failToPutOffset = newMeter("failToPutOffsetPerSec", "zkstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))

  private val oldTopics = new util.LinkedList[String]

  def open() {
    info("Open ZkOffsetStore...")
    zkClient = ZkUtils.createZkClient(zkServers, zkSessionTimeoutMs, zkConnectionTimeoutMs)
    zkUtils = ZkUtils.apply(zkClient, isZkSecurityEnabled = false)
    // any new topic assigned to this Chaperone after this call does not need resending
    loadAllTopics()
  }

  def getOldTopicCount(): Int = oldTopics.size()

  def getZkClient(): ZkClient = zkClient

  def close() {
    info("Close ZkOffsetStore...")
    if (zkClient != null) {
      zkClient.close()
    }
  }

  def store(topicName: String, offsets: util.HashMap[Int, Long], auditMsgs: util.LinkedList[LocalAuditMsg]) {
    debug("Storing offsets=%s to zookeeper for topic=%s".format(offsets, topicName))

    // retry until done successfully, backpressure is imposed to kafka consumers via Disruptor.
    var retryTimes = 1
    while (true) {
      try {
        // ensure znodes are created. Not in transaction scope
        var largestPartitionId = 0
        val partitionItr = offsets.keySet().iterator()
        while (partitionItr.hasNext) {
          val partitionId = partitionItr.next()
          if (partitionId > largestPartitionId) {
            largestPartitionId = partitionId
          }

          val offsetPath = createZkPath(topicName, partitionId.toString)
          debug("Maybe create offset znode with path=%s".format(offsetPath))
          maybeCreateZkPath(offsetPath)
        }

        // because one topic might be handled by multiple threads, thus use partitionId to avoid msg being overwritten
        val dataPath = createZkPath(topicName, hostMeta.asIdentity + "/" + currentEpochInMs + "/" + largestPartitionId)
        debug("Maybe create AuditMsgs znode with path=%s".format(dataPath))
        maybeCreateZkPath(dataPath)

        val ops = new util.LinkedList[Op]

        // store AuditMsgs into zk with offsets in atomic write; treat zk as the extension of local db
        val auditMsgsInBytes = convertToBytes(auditMsgs)

        debug("Put AuditMsgs size=%d to topic=%s with path=%s".format(auditMsgsInBytes.length, topicName, dataPath))

        ops.add(Op.setData(dataPath, auditMsgsInBytes, -1))

        val entryItr = offsets.entrySet().iterator()
        while (entryItr.hasNext) {
          val entry = entryItr.next()

          val partitionId = entry.getKey
          // commit offset+1 to avoid double-count on last msg while restarting
          val offset = entry.getValue + 1
          val offsetPath = createZkPath(topicName, partitionId.toString)
          debug("Put offset=%d to topic=%s, partition=%d with path=%s".format(offset, topicName, partitionId, offsetPath))
          ops.add(Op.setData(offsetPath, offset.toString.getBytes("UTF-8"), -1))
        }

        // conduct all operations need to be done atomically
        zkClient.multi(ops)
        return
      }
      catch {
        case e: Exception =>
          failToPutOffset.mark()
          val sleepInMs = Math.max(500, Math.min(60000, retryTimes * 500))
          retryTimes += 1
          warn("Got exception to put offsets=%s to topic=%s, retryTimes=%d, sleepInMs=%d".format(
            offsets, topicName, retryTimes, sleepInMs), e)
          Thread.sleep(sleepInMs)
        case t: Throwable =>
          fatal("Got fatal error", t)
          throw t
      }
    }
  }

  private def loadAllTopics() {
    try {
      val topicsPath: String = new ZKGroupDirs(zkGroupId).consumerGroupOffsetsDir
      oldTopics.addAll(zkClient.getChildren(topicsPath))
      info("Got topics=%s under path=%s".format(oldTopics, topicsPath))
    }
    catch {
      case e: ZkNoNodeException =>
        info("There is no topics under offsets path")
    }
  }

  def removeUnsentAuditMsg(topicName: String): Unit = {
    try {
      val dataParentPath = createZkPath(topicName, hostMeta.asIdentity)
      val dataNodes = zkUtils.getChildren(dataParentPath)

      val itr = dataNodes.iterator
      var nodeName: String = null
      while (itr.hasNext) {
        val nodeName = itr.next()
        if (nodeName.toLong < currentEpochInMs) {
          val dataPath = createZkPath(topicName, hostMeta.asIdentity + "/" + nodeName)
          debug("Remove old AuditMsgs path=%s per currentEpoch=%d for topic=%s".format(dataPath, currentEpochInMs, topicName))
          zkUtils.deletePathRecursive(dataPath)
        }
      }
    }
    catch {
      case e: ZkNoNodeException =>
        info("There is no old AuditMsgs in zk to remove")
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t
    }
  }

  def loadAuditMsg(buffer: util.LinkedList[LocalAuditMsg], topicName: String, epoch: String) {
    val dataParentPath = createZkPath(topicName, hostMeta.asIdentity + "/" + epoch)
    val dataNodes = zkUtils.getChildren(dataParentPath)
    val itr = dataNodes.iterator

    var nodeName: String = null
    while (itr.hasNext) {
      val nodeName = itr.next()
      val dataPath = createZkPath(topicName, hostMeta.asIdentity + "/" + epoch + "/" + nodeName)
      val auditMsgsInStr = zkUtils.readData(dataPath)._1
      debug("Got auditMsgsInStr=%s".format(auditMsgsInStr))

      val auditMsgsInJSON = JSON.parseArray(auditMsgsInStr)
      debug("Got list of AuditMsgs=%s for topic=%s from znode=%s".format(auditMsgsInJSON, topicName, dataPath))

      for (idx <- 0 until auditMsgsInJSON.size()) {
        buffer.add(LocalAuditMsg.fromJSON(auditMsgsInJSON.getJSONObject(idx)))
      }
    }
  }

  def getUnsentAuditMsg(buffer: util.LinkedList[LocalAuditMsg]) {
    if (oldTopics.isEmpty) {
      info("All old topics have been resent and cleaned up")
      return
    }

    try {
      val topicName = oldTopics.removeFirst()

      val epochParentPath = createZkPath(topicName, hostMeta.asIdentity)
      val epochNodes = zkUtils.getChildren(epochParentPath)

      info("Got old AuditMsgs=%s per currentEpoch=%d for topic=%s".format(epochNodes, currentEpochInMs, topicName))

      val itr = epochNodes.iterator
      var epoch: String = null
      while (itr.hasNext) {
        val epoch = itr.next()
        // there might be multiple old epochs in the znode if Chaperone restart very frequently in short period
        if (epoch.toLong < currentEpochInMs) {
          loadAuditMsg(buffer, topicName, epoch)
        }
      }
      debug("Got list of AuditMsgs count=%d per currentEpoch=%d for topic=%s".format(buffer.size(), currentEpochInMs, topicName))
    }
    catch {
      case e: ZkNoNodeException =>
        info("There is no old AuditMsgs in zk to resend")
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t
    }
  }

  private def convertToBytes(auditMsgs: util.LinkedList[LocalAuditMsg]): Array[Byte] = {
    val itr = auditMsgs.iterator()
    val msgsInJSON = new JSONArray()
    while (itr.hasNext) {
      val auditMsg = itr.next()
      msgsInJSON.add(auditMsg.toJSON)
    }
    msgsInJSON.toString.getBytes("UTF-8")
  }

  private def createZkPath(topicName: String, suffix: String): String = {
    val topicDirs = new ZKGroupTopicDirs(zkGroupId, topicName)
    topicDirs.consumerOffsetDir + "/" + suffix
  }

  private def maybeCreateZkPath(path: String): Unit = {
    try {
      zkUtils.createPersistentPath(path)
    }
    catch {
      case e: ZkNodeExistsException =>
        debug("Path %s is created already".format(path))
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t
    }
  }
}

