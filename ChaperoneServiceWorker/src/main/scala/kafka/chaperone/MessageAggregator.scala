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
import java.util.concurrent.atomic.AtomicLong

import kafka.utils.{Logging, nonthreadsafe}

/**
 * MessageAggregator builds time buckets for msg from specific topic
 */
@nonthreadsafe
class MessageAggregator(val topicName: String, val timeBucketIntervalInSec: Long, val reportFreqBucketCount: Int,
                        val reportFreqIntervalInMs: Long) extends Logging {

  private var timeBucketsMap = new util.HashMap[Double /*bucket beginTimestamp*/ , TimeBucketMetadata]
  private var timeBucketCount = 0

  // Long is immutable, thus use AtomicLong
  private val offsets = new util.HashMap[Int /*partition Id*/ , AtomicLong](8)

  private var nextReportTimeInMs = 0L
  setNextReportTime()

  private def setNextReportTime(): Unit = {
    nextReportTimeInMs = System.currentTimeMillis() + reportFreqIntervalInMs
  }

  def getAndResetBuffer(): util.HashMap[Double, TimeBucketMetadata] = {
    setNextReportTime()
    val current = timeBucketsMap
    timeBucketsMap = new util.HashMap[Double, TimeBucketMetadata]
    timeBucketCount = 0
    current
  }

  def getAndResetOffsets(): util.HashMap[Int, Long] = {
    val ret = new util.HashMap[Int, Long]
    val itr = offsets.entrySet().iterator()
    while (itr.hasNext) {
      val entry = itr.next
      ret.put(entry.getKey, entry.getValue.longValue())
    }
    offsets.clear()
    ret
  }

  def markNoTimeMsg(msgCount: Int) {
    val nowInSec = System.currentTimeMillis() / 1000.0
    val timeBucket = getTimeBucket(nowInSec)
    timeBucket.noTimeMsgCount += msgCount
  }

  def markMalformedMsg(msgCount: Int) {
    val nowInSec = System.currentTimeMillis() / 1000.0
    val timeBucket = getTimeBucket(nowInSec)
    timeBucket.malformedMsgCount += msgCount
  }

  def track(partition: Int, offset: Long, timestamp: Double, msgCount: Int, msgSize: Long): Boolean = {
    val timeBucket = getTimeBucket(timestamp)

    timeBucket.msgCount += msgCount
    timeBucket.totalBytes += msgSize
    timeBucket.lastMessageTimestampSeenInSec = timestamp
    val currentTimeInMs = System.currentTimeMillis
    val latency = currentTimeInMs - (timestamp * 1000)
    for (i <- 1 to msgCount) {
      timeBucket.latencyStats.addValue(latency)
    }

    // update offsets
    var maxOffset: AtomicLong = offsets.get(partition)
    if (maxOffset == null) {
      maxOffset = new AtomicLong(0)
      offsets.put(partition, maxOffset)
    }
    if (maxOffset.longValue < offset) {
      maxOffset.set(offset)
    }

    if (timeBucketCount >= reportFreqBucketCount) {
      debug("FreqBucketCount is reached freqBucketCount=%d, bucketCount=%d, topic=%s".format(
        reportFreqBucketCount, timeBucketCount, topicName))
    }

    if (currentTimeInMs >= nextReportTimeInMs) {
      debug("FreqTimeInterval is reached currentTimeInMs=%d, nextTimeInMs=%d, topic=%s".format(
        currentTimeInMs, nextReportTimeInMs, topicName))
    }

    timeBucketCount >= reportFreqBucketCount || currentTimeInMs >= nextReportTimeInMs
  }

  def isTimeoutToReport: Boolean = {
    timeBucketCount > 0 && System.currentTimeMillis >= nextReportTimeInMs
  }

  private def getTimeBucket(timestamp: Double): TimeBucketMetadata = {
    val bucketBeginSec: Double = getBucketMapping(timestamp)
    var timeBucket: TimeBucketMetadata = timeBucketsMap.get(bucketBeginSec)
    if (timeBucket == null) {
      timeBucket = new TimeBucketMetadata(bucketBeginSec, bucketBeginSec + timeBucketIntervalInSec)
      timeBucketsMap.put(bucketBeginSec, timeBucket)
      timeBucketCount += 1
    }
    timeBucket
  }

  private def getBucketMapping(timestamp: Double): Double = {
    timestamp - (timestamp % timeBucketIntervalInSec)
  }
}
