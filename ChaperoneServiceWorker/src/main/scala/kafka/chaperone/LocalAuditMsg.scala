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

import java.util.concurrent.atomic.AtomicLong

import com.alibaba.fastjson.JSONObject

/**
 * TimeBucketMetadata -> LocalAuditMsg -> Array[Byte] to send
 * LocalAuditMsg is consistent to the table schema. it is persisted to zookeeper and
 * used during resending.
 */
case class LocalAuditMsg(uuid: String, timeBeginInSec: Double, timeEndInSec: Double,
                         topicName: String, hostName: String, datacenter: String, tier: String,
                         totalBytes: Long, count: Long, noTimeCount: Long, malformedCount: Long,
                         meanLatency: Double, latency95: Double, latency99: Double, maxLatency: Double) {

  def getMetricsAsJSON: String = {
    ("{\"totalBytes\":%d, \"count\":%d, \"noTimeCount\":%d, \"malformedCount\":%d, " +
      "\"meanLatency\": %f, \"latency95\": %f, \"latency99\": %f, \"maxLatency\": %f}").format(
      totalBytes, count, noTimeCount, malformedCount, meanLatency, latency95, latency99, maxLatency)
  }

  def toJSON: JSONObject = {
    val ret = new JSONObject()
    ret.put("uuid", uuid)
    ret.put("timeBeginInSec", timeBeginInSec)
    ret.put("timeEndInSec", timeEndInSec)
    ret.put("topicName", topicName)
    ret.put("hostName", hostName)
    ret.put("datacenter", datacenter)
    ret.put("tier", tier)

    ret.put("totalBytes", totalBytes)
    ret.put("count", count)
    ret.put("noTimeCount", noTimeCount)
    ret.put("malformedCount", malformedCount)

    ret.put("meanLatency", meanLatency)
    ret.put("latency95", latency95)
    ret.put("latency99", latency99)
    ret.put("maxLatency", maxLatency)
    ret
  }
}

object LocalAuditMsg {
  private val startEpochMs = System.currentTimeMillis()
  private val sequence = new AtomicLong(1)

  def nextSuffix(): String = {
    startEpochMs + "_" + sequence.incrementAndGet()
  }

  def fromJSON(json: JSONObject): LocalAuditMsg = {
    new LocalAuditMsg(json.getString("uuid"), json.getDoubleValue("timeBeginInSec"), json.getDoubleValue("timeEndInSec"),
      json.getString("topicName"), json.getString("hostName"), json.getString("datacenter"), json.getString("tier"),
      json.getLongValue("totalBytes"), json.getLongValue("count"), json.getLongValue("noTimeCount"), json.getLongValue("malformedCount"),
      json.getDoubleValue("meanLatency"), json.getDoubleValue("latency95"), json.getDoubleValue("latency99"), json.getDoubleValue("maxLatency"))
  }
}

