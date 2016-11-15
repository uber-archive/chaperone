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
package com.uber.stream.kafka.chaperone.collector.reporter;

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.chaperone.collector.AuditMsgField;

/**
 * TimeBucket is to gather count/mean/mean95 for the specific time interval
 * Each TimeBucket is self contained.
 */
public class TimeBucket {
  public final String topicName;
  public final String hostName;
  public final String tier;
  public final String datacenter;
  public final long timeBeginInSec;
  public final long timeEndInSec;
  public double latency95;
  public double latency99;
  public double maxLatency;
  public double totalLatency;
  public long count;
  public long noTimeCount;
  public long malformedCount;
  public long totalBytes;

  public TimeBucket(long timeBeginInSec, long timeEndInSec, String topicName, String hostName, String tier, String dc) {
    this.timeBeginInSec = timeBeginInSec;
    this.timeEndInSec = timeEndInSec;
    this.topicName = topicName;
    this.hostName = hostName;
    this.tier = tier;
    this.datacenter = dc;

    this.totalBytes = 0;

    this.count = 0;
    this.noTimeCount = 0;
    this.malformedCount = 0;

    this.totalLatency = 0;
    this.latency95 = 0;
    this.latency99 = 0;
    this.maxLatency = 0;
  }

  public void addNewRecord(long totalBytes, long counts, long noTimeCount, long malformedCount, double latencyMean,
      double latency95, double latency99, double maxLatency) {
    this.totalBytes += totalBytes;

    this.count += counts;
    this.noTimeCount += noTimeCount;
    this.malformedCount += malformedCount;

    this.totalLatency += latencyMean * counts;
    this.latency95 = Math.max(latency95, this.latency95);
    this.latency99 = Math.max(latency99, this.latency99);
    this.maxLatency = Math.max(maxLatency, this.maxLatency);
  }

  public JSONObject toJSON() {
    JSONObject ret = new JSONObject();
    ret.put(AuditMsgField.TIME_BUCKET_START.getName(), timeBeginInSec);
    ret.put(AuditMsgField.TIME_BUCKET_END.getName(), timeEndInSec);
    ret.put(AuditMsgField.TOPICNAME.getName(), topicName);
    ret.put(AuditMsgField.HOSTNAME.getName(), hostName);
    ret.put(AuditMsgField.TIER.getName(), tier);
    ret.put(AuditMsgField.DATACENTER.getName(), datacenter);

    ret.put(AuditMsgField.METRICS_TOTAL_BYTES.getName(), totalBytes);

    ret.put(AuditMsgField.METRICS_COUNT.getName(), count);
    ret.put(AuditMsgField.METRICS_COUNT_NOTIME.getName(), noTimeCount);
    ret.put(AuditMsgField.METRICS_COUNT_MALFORMED.getName(), malformedCount);

    ret.put(AuditMsgField.METRICS_MEAN_LATENCY.getName(), (count == 0) ? 0 : totalLatency / count);
    ret.put(AuditMsgField.METRICS_P95_LATENCY.getName(), latency95);
    ret.put(AuditMsgField.METRICS_P99_LATENCY.getName(), latency99);
    ret.put(AuditMsgField.METRICS_MAX_LATENCY.getName(), maxLatency);
    return ret;
  }

  public String toString() {
    return toJSON().toString();
  }

  // for instance: {"totalBytes": 10, "count": 10, "noTimeCount": 10, "malformedCount": 10,
  // "mean": 1.0, "mean95": 1.0, "mean99": 1.0, "maxLatency": 1.0}
  public String getMetricsAsJSON() {
    return String.format("{\"totalBytes\":%d, \"count\":%d, \"noTimeCount\":%d, \"malformedCount\":%d, "
        + "\"mean\": %f, \"mean95\": %f, \"mean99\": %f, \"maxLatency\": %f}", totalBytes, count, noTimeCount,
        malformedCount, (count == 0) ? 0 : totalLatency / count, latency95, latency99, maxLatency);
  }
}
