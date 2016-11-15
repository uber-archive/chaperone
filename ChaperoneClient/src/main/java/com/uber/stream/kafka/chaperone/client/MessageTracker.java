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

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageTracker {
  private static final Logger logger = LoggerFactory.getLogger(MessageTracker.class);
  private final int reportFreqBucketCount;
  private final int reportFreqIntervalSec;
  private final int timeBucketIntervalSec;

  private String topicName;

  private ConcurrentMap<Double, TimeBucketMetadata> timeBucketsMap;

  private ReadWriteLock timeBucketsRWLock;

  private final AtomicLong nextReportTime;
  private final AtomicInteger timeBucketCount;
  private final AtomicBoolean reportingInProgress = new AtomicBoolean(false);
  private AuditReporter auditReporter;

  public MessageTracker(String topicName, AuditReporter auditReporter, int timeBucketIntervalSec,
      int reportFreqBucketCount, int reportFreqIntervalSec) {
    this.auditReporter = auditReporter;
    this.topicName = topicName;
    timeBucketsMap = new ConcurrentHashMap<>();
    timeBucketsRWLock = new ReentrantReadWriteLock();
    this.timeBucketIntervalSec = timeBucketIntervalSec;
    this.reportFreqIntervalSec = reportFreqIntervalSec;
    this.reportFreqBucketCount = reportFreqBucketCount;
    timeBucketCount = new AtomicInteger(0);
    nextReportTime = new AtomicLong(setNextReportingTime());
    reportingInProgress.set(false);
    logger.debug("Started auditing for topic {}", this.topicName);
  }

  public void track(double timestamp) {
    track(timestamp, 1);
  }

  public void track(double timestamp, int msgCount) {
    long currentTimeMillis = 0;
    timeBucketsRWLock.readLock().lock();
    try {
      TimeBucketMetadata timeBucket = getTimeBucket(timestamp);
      timeBucket.msgCount.addAndGet(msgCount);

      currentTimeMillis = System.currentTimeMillis();

      timeBucket.lastMessageTimestampSeenInSec = timestamp;
      double latency = currentTimeMillis - (timestamp * 1000);

      for (int i = 0; i < msgCount; i++) {
        timeBucket.latencyStats.addValue(latency);
      }
    } finally {
      timeBucketsRWLock.readLock().unlock();
    }

    if (timeBucketCount.get() >= reportFreqBucketCount || currentTimeMillis > nextReportTime.get()) {
      report();
    }
  }

  public boolean report() {
    if (reportingInProgress.getAndSet(true)) {
      logger.info("Reporting already in progress for {}", topicName);
      return false;
    }

    Map<Double, TimeBucketMetadata> tempTimeCountsMap;
    timeBucketsRWLock.writeLock().lock();
    try {
      tempTimeCountsMap = timeBucketsMap;
      timeBucketCount.set(0);
      timeBucketsMap = new ConcurrentHashMap<>();
    } finally {
      timeBucketsRWLock.writeLock().unlock();
    }

    try {
      for (TimeBucketMetadata bucket : tempTimeCountsMap.values()) {
        AuditMessage m = auditReporter.buildAuditMessage(topicName, bucket);
        auditReporter.reportAuditMessage(m);
      }
    } catch (IOException ioe) {
      logger.error("IOException when trying to send audit message: {}", ioe.toString());
    }

    nextReportTime.set(setNextReportingTime());
    reportingInProgress.set(false);
    return true;
  }

  public void shutdown() {
    report();
  }

  private TimeBucketMetadata getTimeBucket(double timestamp) {
    timeBucketsRWLock.readLock().lock();
    try {
      double bucket = getBucketMapping(timestamp);
      if (!timeBucketsMap.containsKey(bucket)) {
        if (null == timeBucketsMap.putIfAbsent(bucket, new TimeBucketMetadata(bucket, bucket + timeBucketIntervalSec))) {
          timeBucketCount.incrementAndGet();
        }
      }
      return timeBucketsMap.get(bucket);
    } finally {
      timeBucketsRWLock.readLock().unlock();
    }
  }

  private long setNextReportingTime() {
    return System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(reportFreqIntervalSec);
  }

  @VisibleForTesting
  public double getBucketMapping(double timestamp) {
    return timestamp - (timestamp % timeBucketIntervalSec);
  }

  @VisibleForTesting
  public ConcurrentMap<Double, TimeBucketMetadata> getTimeBucketsMap() {
    timeBucketsRWLock.readLock().lock();
    try {
      return timeBucketsMap;
    } finally {
      timeBucketsRWLock.readLock().unlock();
    }
  }
}
