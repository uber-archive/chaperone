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

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.concurrent.atomic.AtomicLong;

public class TimeBucketMetadata {
  public double timeBucketStartInSec;
  public double timeBucketEndInSec;
  public AtomicLong msgCount;
  public double lastMessageTimestampSeenInSec;
  public SynchronizedDescriptiveStatistics latencyStats;

  public TimeBucketMetadata(double start, double end) {
    timeBucketStartInSec = start;
    timeBucketEndInSec = end;
    msgCount = new AtomicLong();
    lastMessageTimestampSeenInSec = 0;
    latencyStats = new SynchronizedDescriptiveStatistics(1000 /* window */);
  }
}
