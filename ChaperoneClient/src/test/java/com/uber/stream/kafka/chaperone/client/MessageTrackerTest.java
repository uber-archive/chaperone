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
package com.uber.stream.kafka.chaperone.client;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

public class MessageTrackerTest {
  MessageTracker messageTracker;
  double testTimestampInSec = 1427778515.9834558964;
  int bucketIntervalInSec = 600;

  private static final Logger logger = LoggerFactory.getLogger(MessageTrackerTest.class);

  @Before
  public void setUp() throws Exception {
    messageTracker = new MessageTracker("test-topic", new AuditReporter() {
      @Override
      public AuditMessage buildAuditMessage(String topicName, TimeBucketMetadata timeBucket) {
        return null;
      }

      @Override
      public void reportAuditMessage(AuditMessage message) throws IOException {}

      @Override
      public void shutdown() {}
    }, bucketIntervalInSec, 123, 60);
  }

  @Test
  public void testTrackOnce() throws Exception {
    messageTracker.track(testTimestampInSec);
    ConcurrentMap<Double, TimeBucketMetadata> buckets = messageTracker.getTimeBucketsMap();
    assertEquals(1, buckets.get(messageTracker.getBucketMapping(testTimestampInSec)).msgCount.get());
  }

  @Test
  public void testLastSeen() throws Exception {
    messageTracker.track(testTimestampInSec);
    ConcurrentMap<Double, TimeBucketMetadata> buckets = messageTracker.getTimeBucketsMap();
    assertEquals(testTimestampInSec,
        buckets.get(messageTracker.getBucketMapping(testTimestampInSec)).lastMessageTimestampSeenInSec, 0.001);
  }

  @Test
  public void testTrackMultiple() throws Exception {
    long totalMessages = 10;
    for (int i = 1; i <= totalMessages; i++) {
      messageTracker.track(testTimestampInSec + i);
    }
    ConcurrentMap<Double, TimeBucketMetadata> buckets = messageTracker.getTimeBucketsMap();
    assertEquals(totalMessages, buckets.get(messageTracker.getBucketMapping(testTimestampInSec)).msgCount.get());
  }

  @Test
  public void testTrackReport() throws Exception {
    messageTracker.track(testTimestampInSec);
    ConcurrentMap<Double, TimeBucketMetadata> buckets = messageTracker.getTimeBucketsMap();
    assertEquals(1, buckets.get(messageTracker.getBucketMapping(testTimestampInSec)).msgCount.get());
    messageTracker.report();

    buckets = messageTracker.getTimeBucketsMap();
    assertEquals(0, buckets.size());
  }

  @Test
  public void testTrackMultipleBuckets() throws Exception {
    int totalMessages = 10;
    for (int i = 0; i < totalMessages; i++) {
      messageTracker.track(testTimestampInSec + 60000 * i);
    }
    // 10 time-buckets seen so far.
    ConcurrentMap<Double, TimeBucketMetadata> buckets = messageTracker.getTimeBucketsMap();
    assertEquals(totalMessages, buckets.size());
    messageTracker.report();

    // All were cleared
    buckets = messageTracker.getTimeBucketsMap();
    assertEquals(0, buckets.size());
  }

  @Test
  public void testReportMultiple() throws Exception {
    messageTracker.track(testTimestampInSec);
    // idempotent
    assertEquals(true, messageTracker.report());
    assertEquals(true, messageTracker.report());
  }
}
