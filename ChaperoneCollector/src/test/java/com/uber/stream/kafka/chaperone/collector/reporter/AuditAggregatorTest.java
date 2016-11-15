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

import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuditAggregatorTest {
  private static final String AUDIT_TOPIC = "chaperone-audit";

  @Test
  public void testAddNewRecordReachCount() {
    long timeBucketIntervalInSec = 60;
    int reportFreqMsgCount = 10;
    short reportFreqIntervalSec = 60;
    AuditAggregator agg = new AuditAggregator(timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec);

    int partition = 0;
    long offset = 1024;

    long nowInMs = System.currentTimeMillis();
    JSONObject record = createRecordMock("mytopic", 2, 5, 95, nowInMs);

    EasyMock.replay(record);

    for (int i = 0; i < 4; i++) {
      assertFalse(agg.addRecord(AUDIT_TOPIC, partition, offset, record));
    }

    assertTrue(agg.addRecord(AUDIT_TOPIC, partition, offset, record));

    EasyMock.verify(record);
  }

  @Test
  public void testAddNewRecordTimeout() throws InterruptedException {
    long timeBucketIntervalInSec = 60;
    int reportFreqMsgCount = 10;
    short reportFreqIntervalSec = 2;
    AuditAggregator agg = new AuditAggregator(timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec);

    int partition = 0;
    long offset = 1024;

    long nowInMs = System.currentTimeMillis();
    JSONObject record = createRecordMock("mytopic", 2, 5, 95, nowInMs);

    EasyMock.replay(record);

    assertFalse(agg.addRecord(AUDIT_TOPIC, partition, offset, record));

    Thread.sleep(3000);

    assertTrue(agg.addRecord(AUDIT_TOPIC, partition, offset, record));

    EasyMock.verify(record);
  }

  @Test
  public void testUpdateOffsets() throws InterruptedException {
    long timeBucketIntervalInSec = 60;
    int reportFreqMsgCount = 10;
    short reportFreqIntervalSec = 3;
    AuditAggregator agg = new AuditAggregator(timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec);

    long nowInMs = System.currentTimeMillis();
    JSONObject record = createRecordMock("mytopic", 2, 5, 95, nowInMs);

    EasyMock.replay(record);

    agg.addRecord(AUDIT_TOPIC, 0, 1, record);
    agg.addRecord(AUDIT_TOPIC, 1, 1, record);
    agg.addRecord(AUDIT_TOPIC, 2, 3, record);
    agg.addRecord(AUDIT_TOPIC, 2, 5, record);
    agg.addRecord(AUDIT_TOPIC, 0, 2, record);
    agg.addRecord(AUDIT_TOPIC, 1, 7, record);

    Map<String, Map<Integer, Long>> topicOffsetsMap = agg.getOffsets();
    Map<Integer, Long> offsets = topicOffsetsMap.get(AUDIT_TOPIC);
    assertEquals(offsets.get(0).longValue(), 2);
    assertEquals(offsets.get(1).longValue(), 7);
    assertEquals(offsets.get(2).longValue(), 5);

    EasyMock.verify(record);
  }

  @Test
  public void testSingleBucket() throws InterruptedException {
    long timeBucketIntervalInSec = 60;
    int reportFreqMsgCount = 10;
    short reportFreqIntervalSec = 3;
    AuditAggregator agg = new AuditAggregator(timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec);
    long nowInMs = System.currentTimeMillis();

    JSONObject record1 = createRecordMock("mytopic", 13, 5, 95, nowInMs, 111111, 7, 6, 97, 103);
    JSONObject record2 = createRecordMock("mytopic", 14, 6, 100, nowInMs, 222222, 2, 12, 105, 115);

    EasyMock.replay(record1, record2);

    agg.addRecord(AUDIT_TOPIC, 0, 1, record1);
    agg.addRecord(AUDIT_TOPIC, 0, 1, record2);

    Map<Long, Map<String, TimeBucket>> buffer = agg.getAndResetBuffer();
    assertEquals(buffer.size(), 1);
    // buffer is reset
    assertEquals(agg.getAndResetBuffer().size(), 0);

    int count = 0;
    for (Map<String, TimeBucket> entry : buffer.values()) {
      for (TimeBucket bucket : entry.values()) {
        assertEquals(bucket.totalBytes, 333333);
        assertEquals(bucket.count, 27);
        assertEquals(bucket.noTimeCount, 9);
        assertEquals(bucket.malformedCount, 18);
        assertEquals(bucket.totalLatency, (13 * 5) + (14 * 6), 0.01);
        assertEquals(bucket.latency95, 100, 0.01);
        assertEquals(bucket.latency99, 105, 0.01);
        assertEquals(bucket.maxLatency, 115, 0.01);
        count++;
      }
    }
    assertEquals(count, 1);

    EasyMock.verify(record1, record2);
  }


  @Test
  public void testMultipleBuckets() throws InterruptedException {
    long timeBucketIntervalInSec = 60;
    int reportFreqMsgCount = 10;
    short reportFreqIntervalSec = 3;
    AuditAggregator agg = new AuditAggregator(timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec);
    long nowInMs = System.currentTimeMillis();

    JSONObject record1 = createRecordMock("mytopic1", 13, 5, 95, nowInMs - 60000 * 2);
    JSONObject record2 = createRecordMock("mytopic2", 14, 5, 95, nowInMs);

    EasyMock.replay(record1, record2);

    agg.addRecord(AUDIT_TOPIC, 0, 1, record1);
    agg.addRecord(AUDIT_TOPIC, 0, 1, record2);

    Map<Long, Map<String, TimeBucket>> buffer = agg.getAndResetBuffer();
    assertEquals(buffer.size(), 2);
    // buffer is reset
    assertEquals(agg.getAndResetBuffer().size(), 0);

    for (Map<String, TimeBucket> entry : buffer.values()) {
      for (TimeBucket bucket : entry.values()) {
        if (bucket.topicName.equals("mytopic1")) {
          assertEquals(bucket.count, 13);
        }

        if (bucket.topicName.equals("mytopic2")) {
          assertEquals(bucket.count, 14);
        }
      }
    }

    EasyMock.verify(record1, record2);
  }

  private JSONObject createRecordMock(String topicName, long count, double meanLatency, double p95Latency, long nowInMs) {
    return createRecordMock(topicName, count, meanLatency, p95Latency, nowInMs, 0, 0, 0, 0, 0);
  }

  private JSONObject createRecordMock(String topicName, long count, double meanLatency, double p95Latency,
      long nowInMs, long totalBytes, long noTimeCount, long malformedCount, double p99Latency, double maxLatency) {
    JSONObject record = EasyMock.createMock(JSONObject.class);
    EasyMock.expect(record.getString(AuditMsgField.TOPICNAME.getName())).andStubReturn(topicName);
    EasyMock.expect(record.getString(AuditMsgField.HOSTNAME.getName())).andStubReturn("myhostname");
    EasyMock.expect(record.getString(AuditMsgField.TIER.getName())).andStubReturn("mytier");
    EasyMock.expect(record.getString(AuditMsgField.DATACENTER.getName())).andStubReturn("mydc");
    EasyMock.expect(record.getLongValue(AuditMsgField.METRICS_COUNT.getName())).andStubReturn(count);
    EasyMock.expect(record.getDoubleValue(AuditMsgField.METRICS_MEAN_LATENCY.getName())).andStubReturn(meanLatency);
    EasyMock.expect(record.getDoubleValue(AuditMsgField.METRICS_P95_LATENCY.getName())).andStubReturn(p95Latency);
    EasyMock.expect(record.getDoubleValue(AuditMsgField.TIME_BUCKET_START.getName())).andStubReturn((double) nowInMs);

    EasyMock.expect(record.getLongValue(AuditMsgField.METRICS_TOTAL_BYTES.getName())).andStubReturn(totalBytes);
    EasyMock.expect(record.getLongValue(AuditMsgField.METRICS_COUNT_NOTIME.getName())).andStubReturn(noTimeCount);
    EasyMock.expect(record.getLongValue(AuditMsgField.METRICS_COUNT_MALFORMED.getName())).andStubReturn(malformedCount);
    EasyMock.expect(record.getDoubleValue(AuditMsgField.METRICS_P99_LATENCY.getName())).andStubReturn(p99Latency);
    EasyMock.expect(record.getDoubleValue(AuditMsgField.METRICS_MAX_LATENCY.getName())).andStubReturn(maxLatency);

    return record;
  }
}
