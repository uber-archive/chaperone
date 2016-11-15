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
import com.codahale.metrics.Meter;
import com.uber.stream.kafka.chaperone.collector.AuditMsgField;
import com.uber.stream.kafka.chaperone.collector.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregator accumulates audit msg for each topic by its timestamp.
 *
 * One key configuration is TimeInterval used to group.
 * 1. The start/end timestamp of each record are aligned to it.
 * 2. TimeInterval controls the granularity of bucket, which tradeoff IO/storage overhead and
 * precision.
 * 3. TimeInterval is not recommended to change frequently. If it's changed, expect audit error
 * during transition.
 * 4. TimeInterval has to be no smaller than the one used by chaperone service.
 */
public class AuditAggregator {
  private static final Logger logger = LoggerFactory.getLogger(AuditAggregator.class);

  // the key is (topic.host.tier.dc)
  public static final String KEY_FORMAT = "%s|%s|%s|%s";

  private final long timeBucketIntervalInSec;
  private final int reportFreqMsgCount;
  private final int reportFreqIntervalSec;
  private final boolean combineMetricsAmongHosts;
  private long nextReportTimeInMs;

  // 999999999999 (12 digits), as millisecond, means Sun Sep 09 2001 01:46:39 UTC.
  // Thus, all today's time, as in ms, is larger than this number. Besides,
  // 1000000000000 (13 digits), as second, means Fri Sep 27 33658 01:46:40....
  private final static long MIN_TIMESTAMP_MS = 999999999999L;

  private Map<Long/* bucketTimeInSec */, Map<String, TimeBucket>> buffer = new HashMap<>();

  // Long is immutable, thus use AtomicLong
  private Map<String, Map<Integer, AtomicLong>> topicOffsetsMap = new HashMap<>();

  private final Meter INVALID_TIMESTAMP_COUNTER;
  private double timestampSkewSec = TimeUnit.DAYS.toSeconds(31);

  public AuditAggregator(long timeBucketIntervalInSec, int reportFreqMsgCount, int reportFreqIntervalSec) {
    this(timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec, false);
  }

  public AuditAggregator(long timeBucketIntervalInSec, int reportFreqMsgCount, int reportFreqIntervalSec,
      boolean combineMetricsAmongHosts) {
    this.timeBucketIntervalInSec = timeBucketIntervalInSec;
    this.reportFreqMsgCount = reportFreqMsgCount;
    this.reportFreqIntervalSec = reportFreqIntervalSec;
    this.combineMetricsAmongHosts = combineMetricsAmongHosts;
    INVALID_TIMESTAMP_COUNTER = Metrics.getRegistry().meter("auditAggregator.invalidTimestampNumber");
    setNextReportingTime();
  }

  public boolean addRecord(String sourceTopic, int partition, long offset, JSONObject record) {
    // dot . is reserved delimiter for graphite naming
    String topicName = record.getString(AuditMsgField.TOPICNAME.getName());
    String hostName = record.getString(AuditMsgField.HOSTNAME.getName());
    String tier = record.getString(AuditMsgField.TIER.getName());
    String dc = record.getString(AuditMsgField.DATACENTER.getName());

    if (combineMetricsAmongHosts) {
      hostName = "combined";
    }

    String key = String.format(KEY_FORMAT, topicName, hostName, tier, dc);

    double timestampInSec = record.getDoubleValue(AuditMsgField.TIME_BUCKET_START.getName());
    // just in case. In fact, chaperone service has logic to convert ms to sec
    // change millisecond to second for consistency
    if (timestampInSec > MIN_TIMESTAMP_MS) {
      timestampInSec /= 1000;
    }

    // add more defense code to avoid weird timestamp stopping ingester
    if (timestampInSec <= 0 || timestampInSec >= (timestampSkewSec + System.currentTimeMillis() / 1000.0)) {
      logger.warn(String.format("Got very skewed timestamp from %s as %f", topicName, timestampInSec));
      INVALID_TIMESTAMP_COUNTER.mark();
      return false;
    }

    long totalCount = record.getLongValue(AuditMsgField.METRICS_COUNT.getName());
    double meanLatency = record.getDoubleValue(AuditMsgField.METRICS_MEAN_LATENCY.getName());
    double p95Latency = record.getDoubleValue(AuditMsgField.METRICS_P95_LATENCY.getName());

    long totalBytes = record.getLongValue(AuditMsgField.METRICS_TOTAL_BYTES.getName());
    long noTimeCount = record.getLongValue(AuditMsgField.METRICS_COUNT_NOTIME.getName());
    long malformedCount = record.getLongValue(AuditMsgField.METRICS_COUNT_MALFORMED.getName());
    double p99Latency = record.getDoubleValue(AuditMsgField.METRICS_P99_LATENCY.getName());
    double maxLatency = record.getDoubleValue(AuditMsgField.METRICS_MAX_LATENCY.getName());

    long bucketTimeInSec = (long) (timestampInSec - timestampInSec % timeBucketIntervalInSec);

    Map<String, TimeBucket> timeBuckets = buffer.get(bucketTimeInSec);
    if (timeBuckets == null) {
      timeBuckets = new HashMap<>();
      buffer.put(bucketTimeInSec, timeBuckets);
    }

    // update time bucket
    TimeBucket timeBucket = timeBuckets.get(key);
    if (timeBucket == null) {
      timeBucket =
          new TimeBucket(bucketTimeInSec, bucketTimeInSec + timeBucketIntervalInSec, topicName, hostName, tier, dc);
      timeBuckets.put(key, timeBucket);
    }
    timeBucket.addNewRecord(totalBytes, totalCount, noTimeCount, malformedCount, meanLatency, p95Latency, p99Latency,
        maxLatency);

    // update offsets
    updateOffset(sourceTopic, partition, offset);

    long bucketedMsgCount = timeBucket.count;
    if (bucketedMsgCount >= reportFreqMsgCount) {
      logger.info(String.format("FreqMsgCount is reached for freqMsgCount=%d, bucketedMsgCount=%d", reportFreqMsgCount,
          bucketedMsgCount));
    }

    long currentTimeInMs = System.currentTimeMillis();
    if (currentTimeInMs > nextReportTimeInMs) {
      logger.info(String.format("FreqTimeInterval is reached currentTimeInMs=%d, nextTimeInMs=%d", currentTimeInMs,
          nextReportTimeInMs));
    }

    return (bucketedMsgCount >= reportFreqMsgCount || currentTimeInMs > nextReportTimeInMs);
  }

  private void updateOffset(String sourceTopic, int partition, long offset) {
    Map<Integer, AtomicLong> offsets = topicOffsetsMap.get(sourceTopic);
    if (offsets == null) {
      offsets = new HashMap<>();
      topicOffsetsMap.put(sourceTopic, offsets);
    }

    AtomicLong maxOffset = offsets.get(partition);
    if (maxOffset == null) {
      maxOffset = new AtomicLong(0);
      offsets.put(partition, maxOffset);
    }

    // make sure offsets just increases
    if (maxOffset.longValue() < offset) {
      maxOffset.set(offset);
    }
  }

  private void setNextReportingTime() {
    nextReportTimeInMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(reportFreqIntervalSec);
  }

  public Map<Long/* begin_time */, Map<String, TimeBucket>> getAndResetBuffer() {
    setNextReportingTime();
    Map<Long, Map<String, TimeBucket>> current = buffer;
    buffer = new HashMap<>();
    return current;
  }

  /**
   * deep copy to avoid accidentally modification on offsets
   */
  public Map<String, Map<Integer, Long>> getOffsets() {
    Map<String, Map<Integer, Long>> ret = new HashMap<>(8);
    for (Map.Entry<String, Map<Integer, AtomicLong>> topicEntry : topicOffsetsMap.entrySet()) {
      Map<Integer, Long> offsets = new HashMap<>();
      ret.put(topicEntry.getKey(), offsets);
      for (Map.Entry<Integer, AtomicLong> offsetEntry : topicEntry.getValue().entrySet()) {
        offsets.put(offsetEntry.getKey(), offsetEntry.getValue().longValue());
      }
    }
    return ret;
  }
}
