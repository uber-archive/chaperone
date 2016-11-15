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

import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics

class TimeBucketMetadata(val timeBucketStartInSec: Double,
                         val timeBucketEndInSec: Double) {
  var uuid: String = "undefined"
  var totalBytes = 0L
  var msgCount = 0L
  var noTimeMsgCount = 0L
  var malformedMsgCount = 0L
  var lastMessageTimestampSeenInSec: Double = 0
  var latencyStats = new SynchronizedDescriptiveStatistics(1000 /*window*/)

  override def toString: String = {
    val str = new StringBuilder()
    str.append("timeBucket=[")
    str.append("uuid=").append(uuid).append(",")
    str.append("timeBucketStartInSec=").append(timeBucketStartInSec).append(",")
    str.append("timeBucketEndInSec=").append(timeBucketEndInSec).append(",")
    str.append("lastMessageTimestampSeenInSec=").append(lastMessageTimestampSeenInSec).append(",")
    str.append("totalBytes=").append(totalBytes).append(",")
    str.append("count=").append(msgCount).append(",")
    str.append("noTimeCount=").append(noTimeMsgCount).append(",")
    str.append("malformedCount=").append(malformedMsgCount).append(",")
    str.append("meanLatency=").append(latencyStats.getMean).append(",")
    str.append("latency95=").append(latencyStats.getPercentile(95)).append("]")
    str.append("latency99=").append(latencyStats.getPercentile(99)).append("]")
    str.append("maxLatency=").append(latencyStats.getMax).append("]")
    str.toString()
  }

  def toLocalAuditMsg(newUUID: String, topicName: String, hostMeta: HostMetadata): LocalAuditMsg = {
    new LocalAuditMsg(newUUID, timeBucketStartInSec, timeBucketEndInSec,
      topicName, hostMeta.host, hostMeta.datacenter, hostMeta.tier,
      totalBytes, msgCount, noTimeMsgCount, malformedMsgCount,
      latencyStats.getMean, latencyStats.getPercentile(95), latencyStats.getPercentile(99), latencyStats.getMax)
  }
}
