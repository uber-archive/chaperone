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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.Util;
import com.uber.stream.kafka.chaperone.collector.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class AuditReporter implements IAuditReporter {
  private static final Logger logger = LoggerFactory.getLogger(AuditReporter.class);

  private final ExecutorService reportExecutor;
  private Disruptor<AuditMsgReportTask> disruptor;
  private RingBuffer<AuditMsgReportTask> ringBuffer;

  // msg in/out rate and disruptor queue size
  private final Meter SUBMITTED_COUNTER;
  private final Meter FAILED_TO_SUBMIT_COUNTER;

  // not 'final' for test purpose
  AuditAggregator aggregator;
  final Meter REPORTED_COUNTER;
  final Meter FAILED_TO_REPORT_COUNTER;

  static abstract class AuditReporterBuilder {
    int queueSize;
    long timeBucketIntervalInSec;
    int reportFreqMsgCount;
    int reportFreqIntervalSec;
    boolean combineMetricsAmongHosts;

    AuditReporterBuilder setQueueSize(int queueSize) {
      this.queueSize = queueSize;
      return this;
    }

    AuditReporterBuilder setTimeBucketIntervalInSec(long timeBucketIntervalInSec) {
      this.timeBucketIntervalInSec = timeBucketIntervalInSec;
      return this;
    }

    AuditReporterBuilder setReportFreqMsgCount(int reportFreqMsgCount) {
      this.reportFreqMsgCount = reportFreqMsgCount;
      return this;
    }

    AuditReporterBuilder setReportFreqIntervalSec(int reportFreqIntervalSec) {
      this.reportFreqIntervalSec = reportFreqIntervalSec;
      return this;
    }

    AuditReporterBuilder setCombineMetricsAmongHosts(boolean combineMetricsAmongHosts) {
      this.combineMetricsAmongHosts = combineMetricsAmongHosts;
      return this;
    }

    public abstract IAuditReporter build();
  }

  AuditReporter(int queueSize, long timeBucketIntervalInSec, int reportFreqMsgCount, int reportFreqIntervalSec,
      boolean combineMetricsAmongHosts) {
    reportExecutor =
        Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(getType() + "-audit-reporter-%d")
            .build());;

    queueSize = Util.ceilingNextPowerOfTwo(queueSize);
    disruptor = new Disruptor<AuditMsgReportTask>(new AuditMsgReportTaskFactory(), queueSize, reportExecutor);
    disruptor.handleEventsWith(new AuditMsgReportTaskHandler(this));
    ringBuffer = disruptor.getRingBuffer();

    aggregator =
        new AuditAggregator(timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec,
            combineMetricsAmongHosts);

    SUBMITTED_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.submittedNumber");
    FAILED_TO_SUBMIT_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.failedToSubmitNumber");
    REPORTED_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.reportedNumber");
    FAILED_TO_REPORT_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.failedToReportNumber");
    Metrics.getRegistry().register(getType() + ".auditReporter.queueSize", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return (int) disruptor.getRingBuffer().remainingCapacity();
      }
    });
  }

  @Override
  public void start() {
    logger.info(String.format("Starting %s auditMsg reporter...", getType()));
    disruptor.start();
  }

  @Override
  public void shutdown() {
    logger.info(String.format("Stopping %s auditMsg reporter...", getType()));
    disruptor.shutdown();
    reportExecutor.shutdown();
  }

  @Override
  public void submit(String sourceTopic, int partition, long offset, JSONObject record) {
    try {
      logger.debug("Submitting auditMsg from sourceTopic={}, partition={}, offset={}, record={}", sourceTopic,
          partition, offset, record);
      // blocking to publish; impose back pressure if queue is full
      ringBuffer.publishEvent(TRANSLATOR, new AuditMsgReportTask(sourceTopic, partition, offset, record));
      SUBMITTED_COUNTER.mark();
    } catch (Exception e) {
      FAILED_TO_SUBMIT_COUNTER.mark();
      throw e;
    }
  }

  public abstract void report(String sourceTopic, int partition, long offset, JSONObject record)
      throws InterruptedException;

  public abstract String getType();

  // helper classes for Disruptor
  static class AuditMsgReportTask {
    private String sourceTopic;
    private int partition;
    private long offset;
    private JSONObject record;

    AuditMsgReportTask() {}

    AuditMsgReportTask(String sourceTopic, int partition, long offset, JSONObject record) {
      this.sourceTopic = sourceTopic;
      this.partition = partition;
      this.offset = offset;
      this.record = record;
    }
  }

  private static class AuditMsgReportTaskFactory implements EventFactory<AuditMsgReportTask> {
    @Override
    public AuditMsgReportTask newInstance() {
      return new AuditMsgReportTask();
    }
  }

  private static class AuditMsgReportTaskHandler implements EventHandler<AuditMsgReportTask> {
    private final AuditReporter auditReporter;

    AuditMsgReportTaskHandler(final AuditReporter auditReporter) {
      this.auditReporter = auditReporter;
    }

    @Override
    public void onEvent(AuditMsgReportTask task, long sequence, boolean endOfBatch) throws Exception {
      auditReporter.report(task.sourceTopic, task.partition, task.offset, task.record);
      task.record = null;
    }
  }

  private static final EventTranslatorOneArg<AuditMsgReportTask, AuditMsgReportTask> TRANSLATOR =
      new EventTranslatorOneArg<AuditMsgReportTask, AuditMsgReportTask>() {
        @Override
        public void translateTo(AuditMsgReportTask task, long sequence, AuditMsgReportTask paramTask) {
          task.sourceTopic = paramTask.sourceTopic;
          task.partition = paramTask.partition;
          task.offset = paramTask.offset;
          task.record = paramTask.record;
        }
      };
}
