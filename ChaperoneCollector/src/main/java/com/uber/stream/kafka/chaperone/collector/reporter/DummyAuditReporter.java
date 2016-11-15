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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * For local test
 */
class DummyAuditReporter extends AuditReporter {
  private static final Logger logger = LoggerFactory.getLogger(DummyAuditReporter.class);

  private final String dummyStartOffsets;

  static DummyAuditReporterBuilder newBuilder() {
    return new DummyAuditReporterBuilder();
  }

  static class DummyAuditReporterBuilder extends AuditReporterBuilder {
    private String dummyStartOffsets;

    DummyAuditReporterBuilder setDummyStartOffsets(String dummyStartOffsets) {
      this.dummyStartOffsets = dummyStartOffsets;
      return this;
    }

    public IAuditReporter build() {
      return new DummyAuditReporter(queueSize, timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec,
          combineMetricsAmongHosts, dummyStartOffsets);
    }
  }

  private DummyAuditReporter(int queueSize, long timeBucketIntervalInSec, int reportFreqMsgCount,
      int reportFreqIntervalSec, boolean combineMetricsAmongHosts, String dummyStartOffsets) {
    super(queueSize, timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec, combineMetricsAmongHosts);
    this.dummyStartOffsets = dummyStartOffsets;
  }

  @Override
  public void report(String sourceTopic, int partition, long offset, JSONObject record) {
    logger.info("Got one more sourceTopic={}, partition={}, offset={}, record={}", sourceTopic, partition, offset,
        record);

    if (!aggregator.addRecord(sourceTopic, partition, offset, record)) {
      return;
    }

    Map<Long, Map<String, TimeBucket>> buffer = aggregator.getAndResetBuffer();
    Map<String, Map<Integer, Long>> topicOffsetsMap = aggregator.getOffsets();
    logger.info("Reporting the buffered auditMsgs={} and offsets={}", buffer, topicOffsetsMap);
  }

  @Override
  public String getType() {
    return "DummyAuditReporter";
  }

  /**
   * dummyStartOffsets is like: topicA,111,211,311,411,511,topicB,111,112,113,114
   */
  @Override
  public Map<String, Map<Integer, Long>> getOffsetsToStart() {
    Map<String, Map<Integer, Long>> ret = new HashMap<>();
    int partitionId = 0;
    Map<Integer, Long> offsets = null;
    String[] offsetsInStr = dummyStartOffsets.split(",");
    for (String offsetInStr : offsetsInStr) {
      if (!isLong(offsetInStr)) {
        offsets = new HashMap<>();
        ret.put(offsetInStr, offsets);
        partitionId = 0;
      } else if (offsets != null) {
        offsets.put(partitionId++, Long.valueOf(offsetInStr.trim()));
      }
    }
    return ret;
  }

  private boolean isLong(String numInStr) {
    try {
      Long.parseLong(numInStr);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
