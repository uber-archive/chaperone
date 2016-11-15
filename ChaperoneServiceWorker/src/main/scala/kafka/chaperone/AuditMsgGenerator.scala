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

import com.alibaba.fastjson.JSONObject
import kafka.utils.Logging

object AuditMsgGenerator extends Logging {
  def fromAuditMsgRecord(localAuditMsg: LocalAuditMsg): Array[Byte] = {
    val auditMsg = new JSONObject
    auditMsg.put("topicName", localAuditMsg.topicName)
    auditMsg.put("timeBeginInSec", localAuditMsg.timeBeginInSec)
    auditMsg.put("timeEndInSec", localAuditMsg.timeEndInSec)

    auditMsg.put("totalBytes", localAuditMsg.totalBytes)
    auditMsg.put("count", localAuditMsg.count)
    auditMsg.put("noTimeCount", localAuditMsg.noTimeCount)
    auditMsg.put("malformedCount", localAuditMsg.malformedCount)

    auditMsg.put("meanLatency", localAuditMsg.meanLatency)
    auditMsg.put("latency95", localAuditMsg.latency95)
    auditMsg.put("latency99", localAuditMsg.latency99)
    auditMsg.put("maxLatency", localAuditMsg.maxLatency)

    auditMsg.put("tier", localAuditMsg.tier)
    auditMsg.put("hostname", localAuditMsg.hostName)
    auditMsg.put("datacenter", localAuditMsg.datacenter)

    // uuid is used to deduplicate in the 'audit msg at least once' scenario
    auditMsg.put("uuid", localAuditMsg.uuid)
    auditMsg.toString.getBytes
  }
}
