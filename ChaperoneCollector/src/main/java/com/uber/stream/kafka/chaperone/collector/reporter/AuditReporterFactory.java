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


import com.uber.stream.kafka.chaperone.collector.AuditConfig;

/**
 * Supported types include db, dummy
 */
public class AuditReporterFactory {
  public static final String DB_AUDIT_REPORTER = "db";
  public static final String DUMMY_AUDIT_REPORTER = "dummy";

  public static IAuditReporter getAuditReporter(String type) {
    if (type.equalsIgnoreCase(DB_AUDIT_REPORTER)) {
      return DbAuditReporter.newBuilder().setDbUser(AuditConfig.AUDIT_DB_USERNAME)
          .setDbPass(AuditConfig.AUDIT_DB_PASSWORD).setDbUrl(AuditConfig.AUDIT_DB_JDBC_URL)
          .setDataTableName(AuditConfig.AUDIT_DB_DATA_TABLE_NAME)
          .setOffsetTableName(AuditConfig.AUDIT_DB_OFFSET_TABLE_NAME)
          .setDbRetentionInHr(AuditConfig.AUDIT_DB_RETENTION_HOURS)
          .setEnableRemoveOldRecord(AuditConfig.AUDIT_DB_REMOVE_OLD_RECORD)
          .setQueueSize(AuditConfig.AUDIT_REPORTER_QUEUE_SIZE)
          .setTimeBucketIntervalInSec(AuditConfig.TIME_BUCKET_INTERVAL_SEC)
          .setReportFreqMsgCount(AuditConfig.REPORT_FREQ_MSG_COUNT)
          .setReportFreqIntervalSec(AuditConfig.REPORT_FREQ_INTERVAL_SEC)
          .setCombineMetricsAmongHosts(AuditConfig.INGESTER_COMBINE_METRICS_AMONG_HOSTS).build();

    } else if (type.equalsIgnoreCase(DUMMY_AUDIT_REPORTER)) {
      return DummyAuditReporter.newBuilder().setDummyStartOffsets(AuditConfig.DUMMY_START_OFFSETS)
          .setQueueSize(AuditConfig.AUDIT_REPORTER_QUEUE_SIZE)
          .setTimeBucketIntervalInSec(AuditConfig.TIME_BUCKET_INTERVAL_SEC)
          .setReportFreqMsgCount(AuditConfig.REPORT_FREQ_MSG_COUNT)
          .setReportFreqIntervalSec(AuditConfig.REPORT_FREQ_INTERVAL_SEC)
          .setCombineMetricsAmongHosts(AuditConfig.INGESTER_COMBINE_METRICS_AMONG_HOSTS).build();
    } else {
      throw new IllegalArgumentException("Unknown audit reporter type=" + type);
    }
  }
}
