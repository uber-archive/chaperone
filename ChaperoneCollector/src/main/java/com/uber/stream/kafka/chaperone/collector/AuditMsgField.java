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
package com.uber.stream.kafka.chaperone.collector;

public enum AuditMsgField {
  UUID("uuid"), TIME_BUCKET_START("timeBeginInSec"), TIME_BUCKET_END("timeEndInSec"), TOPICNAME("topicName"), HOSTNAME(
      "hostname"), TIER("tier"), DATACENTER("datacenter"), METRICS_TOTAL_BYTES("totalBytes"), METRICS_COUNT("count"), METRICS_COUNT_NOTIME(
      "noTimeCount"), METRICS_COUNT_MALFORMED("malformedCount"), METRICS_MEAN_LATENCY("meanLatency"), METRICS_P95_LATENCY(
      "latency95"), METRICS_P99_LATENCY("latency99"), METRICS_MAX_LATENCY("maxLatency");

  private String name;

  AuditMsgField(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
