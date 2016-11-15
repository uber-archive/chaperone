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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AuditConfig {
  public static String GRAPHITE_HOST_NAME;
  public static int GRAPHITE_PORT;
  public static long GRAPHITE_REPORT_PERIOD_SEC;

  public static int REPORT_FREQ_MSG_COUNT;
  public static int REPORT_FREQ_INTERVAL_SEC;
  public static int TIME_BUCKET_INTERVAL_SEC;

  public static String AUDIT_TOPIC_NAME;
  public static String DATA_CENTER;

  public static String KAFKA_BROKER_LIST;

  public static boolean ENABLE_PERSISTENT_STORE;

  public static int INGESTER_NUM_WORKER_THREADS;
  public static String INGESTER_ZK_CONNECT;
  public static String INGESTER_ZK_SESSION_TIMEOUT_MS;
  public static String INGESTER_ZK_SYNC_TIME_MS;
  public static String INGESTER_AUTO_OFFSET_RESET;
  public static String INGESTER_AUTO_COMMIT_ENABLE;
  public static String INGESTER_AUTO_COMMIT_INTERVAL_MS;
  public static boolean INGESTER_START_FROM_TAIL;

  public static String INGESTER_BLACKLISTED_TOPICS;
  public static boolean INGESTER_COMBINE_METRICS_AMONG_HOSTS;
  public static boolean INGESTER_ENABLE_DEDUP;
  public static String INGESTER_HOSTS_WITH_DUP;
  public static String INGESTER_DUP_HOST_PREFIX;
  public static String INGESTER_REDIS_HOST;
  public static int INGESTER_REDIS_PORT;
  public static long INGESTER_REDIS_MAXMEM_BYTES;
  public static String INGESTER_REDIS_MAXMEM_POLICY;
  public static int INGESTER_REDIS_KEY_TTL_SEC;
  public static boolean IGNORE_OFFSETS_IN_PERSISTENT_STORE;

  public static String AUDIT_REPORTER_TYPE;
  public static int AUDIT_REPORTER_QUEUE_SIZE;

  public static int AUDIT_DB_RETENTION_HOURS;
  public static String AUDIT_DB_USERNAME;
  public static String AUDIT_DB_PASSWORD;
  public static String AUDIT_DB_JDBC_URL;
  public static String AUDIT_DB_DATA_TABLE_NAME;
  public static String AUDIT_DB_OFFSET_TABLE_NAME;
  public static boolean AUDIT_DB_REMOVE_OLD_RECORD;
  public static String DUMMY_START_OFFSETS;

  public static class NamedConstants {
    public static final String GRAPHITE_HOST_NAME = "metrics.reporter.graphite.host";
    public static final String GRAPHITE_PORT = "metrics.reporter.graphite.port";
    public static final String GRAPHITE_REPORT_PERIOD_SEC = "metrics.reporter.graphite.report.period.sec";

    public static final String REPORT_FREQ_MSG_COUNT = "messageTracker.reportFreqMsgCount";
    public static final String REPORT_FREQ_INTERVAL_SEC = "messageTracker.reportFreqIntervalInSec";
    public static final String TIME_BUCKET_INTERVAL_SEC = "messageTracker.timeBucketIntervalInSec";

    public static final String AUDIT_TOPIC_NAME = "reporter.auditTopicName";
    public static final String DATA_CENTER = "reporter.datacenter";

    public static final String KAFKA_BROKER_LIST = "kafka.brokerList";

    public static final String INGESTER_NUM_WORKER_THREADS = "ingester.numWorkerThreads";
    public static final String INGESTER_ZK_CONNECT = "ingester.zookeeper.connect";
    public static final String INGESTER_ZK_SESSION_TIMEOUT_MS = "ingester.zookeeper.session.timeout.ms";
    public static final String INGESTER_ZK_SYNC_TIME_MS = "ingester.zookeeper.sync.time.ms";
    public static final String INGESTER_AUTO_OFFSET_RESET = "ingester.auto.offset.reset";
    public static final String INGESTER_AUTO_COMMIT_ENABLE = "ingester.auto.commit.enable";
    public static final String INGESTER_AUTO_COMMIT_INTERVAL_MS = "ingester.auto.commit.interval.ms";
    public static final String INGESTER_START_FROM_TAIL = "ingester.start.from.tail";

    public static final String INGESTER_BLACKLISTED_TOPICS = "ingester.blacklisted.topics";
    public static final String INGESTER_COMBINE_METRICS_AMONG_HOSTS = "ingester.combine.metrics.among.hosts";
    public static final String INGESTER_ENABLE_DEDUP = "ingester.enable.dedup";
    public static final String INGESTER_DUP_HOST_PREFIX = "ingester.dup.host.prefix";
    public static final String INGESTER_HOSTS_WITH_DUP = "ingester.hosts.with.dup";
    public static final String INGESTER_REDIS_HOST = "ingester.redis.host";
    public static final String INGESTER_REDIS_PORT = "ingester.redis.port";
    public static final String INGESTER_REDIS_MAXMEM_BYTES = "ingester.redis.maxmem.bytes";
    public static final String INGESTER_REDIS_MAXMEM_POLICY = "ingester.redis.maxmem.policy";
    public static final String INGESTER_REDIS_KEY_TTL_SEC = "ingester.redis.key.ttl.sec";

    public static final String ENABLE_PERSISTENT_STORE = "enable.persistent.store";

    public static final String IGNORE_OFFSETS_IN_PERSISTENT_STORE = "ignore.offsets.in.persistent.store";

    public static final String AUDIT_REPORTER_TYPE = "audit.reporter.type";
    public static final String AUDIT_REPORTER_QUEUE_SIZE = "audit.reporter.queue.size";
    public static final String AUDIT_DB_RETENTION_HOURS = "audit.db.retention.hours";
    public static final String AUDIT_DB_USERNAME = "audit.db.username";
    public static final String AUDIT_DB_PASSWORD = "audit.db.password";
    public static final String AUDIT_DB_JDBC_URL = "audit.db.jdbc.url";
    public static final String AUDIT_DB_DATA_TABLE_NAME = "audit.db.data.table.name";
    public static final String AUDIT_DB_OFFSET_TABLE_NAME = "audit.db.offset.table.name";
    public static final String AUDIT_DB_REMOVE_OLD_RECORD = "audit.db.remove.old.record";
    public static final String DUMMY_START_OFFSETS = "dummy.start.offsets";
  }

  public static void loadConfig() {
    Config config = ConfigFactory.load();
    GRAPHITE_HOST_NAME = config.getString(NamedConstants.GRAPHITE_HOST_NAME);
    GRAPHITE_PORT = config.getInt(NamedConstants.GRAPHITE_PORT);
    GRAPHITE_REPORT_PERIOD_SEC = config.getInt(NamedConstants.GRAPHITE_REPORT_PERIOD_SEC);

    REPORT_FREQ_MSG_COUNT = config.getInt(NamedConstants.REPORT_FREQ_MSG_COUNT);
    REPORT_FREQ_INTERVAL_SEC = (short) config.getInt(NamedConstants.REPORT_FREQ_INTERVAL_SEC);
    TIME_BUCKET_INTERVAL_SEC = (short) config.getInt(NamedConstants.TIME_BUCKET_INTERVAL_SEC);

    AUDIT_TOPIC_NAME = config.getString(NamedConstants.AUDIT_TOPIC_NAME);
    DATA_CENTER = config.getString(NamedConstants.DATA_CENTER);

    KAFKA_BROKER_LIST = config.getString(NamedConstants.KAFKA_BROKER_LIST);

    // Initialize Chaperone service properties
    INGESTER_NUM_WORKER_THREADS = (short) config.getInt(NamedConstants.INGESTER_NUM_WORKER_THREADS);
    INGESTER_ZK_CONNECT = config.getString(NamedConstants.INGESTER_ZK_CONNECT);
    INGESTER_ZK_SESSION_TIMEOUT_MS = config.getString(NamedConstants.INGESTER_ZK_SESSION_TIMEOUT_MS);
    INGESTER_ZK_SYNC_TIME_MS = config.getString(NamedConstants.INGESTER_ZK_SYNC_TIME_MS);
    INGESTER_AUTO_OFFSET_RESET = config.getString(NamedConstants.INGESTER_AUTO_OFFSET_RESET);
    INGESTER_AUTO_COMMIT_ENABLE = config.getString(NamedConstants.INGESTER_AUTO_COMMIT_ENABLE);
    INGESTER_AUTO_COMMIT_INTERVAL_MS = config.getString(NamedConstants.INGESTER_AUTO_COMMIT_INTERVAL_MS);
    INGESTER_START_FROM_TAIL = config.getBoolean(NamedConstants.INGESTER_START_FROM_TAIL);

    ENABLE_PERSISTENT_STORE = config.getBoolean(NamedConstants.ENABLE_PERSISTENT_STORE);
    INGESTER_BLACKLISTED_TOPICS = config.getString(NamedConstants.INGESTER_BLACKLISTED_TOPICS);
    INGESTER_COMBINE_METRICS_AMONG_HOSTS = config.getBoolean(NamedConstants.INGESTER_COMBINE_METRICS_AMONG_HOSTS);
    INGESTER_ENABLE_DEDUP = config.getBoolean(NamedConstants.INGESTER_ENABLE_DEDUP);
    INGESTER_DUP_HOST_PREFIX = config.getString(NamedConstants.INGESTER_DUP_HOST_PREFIX);
    INGESTER_HOSTS_WITH_DUP = config.getString(NamedConstants.INGESTER_HOSTS_WITH_DUP);
    INGESTER_REDIS_HOST = config.getString(NamedConstants.INGESTER_REDIS_HOST);
    INGESTER_REDIS_PORT = config.getInt(NamedConstants.INGESTER_REDIS_PORT);
    INGESTER_REDIS_MAXMEM_BYTES = config.getLong(NamedConstants.INGESTER_REDIS_MAXMEM_BYTES);
    INGESTER_REDIS_MAXMEM_POLICY = config.getString(NamedConstants.INGESTER_REDIS_MAXMEM_POLICY);
    INGESTER_REDIS_KEY_TTL_SEC = config.getInt(NamedConstants.INGESTER_REDIS_KEY_TTL_SEC);
    IGNORE_OFFSETS_IN_PERSISTENT_STORE = config.getBoolean(NamedConstants.IGNORE_OFFSETS_IN_PERSISTENT_STORE);

    AUDIT_REPORTER_TYPE = config.getString(NamedConstants.AUDIT_REPORTER_TYPE);
    AUDIT_REPORTER_QUEUE_SIZE = config.getInt(NamedConstants.AUDIT_REPORTER_QUEUE_SIZE);
    AUDIT_DB_RETENTION_HOURS = config.getInt(NamedConstants.AUDIT_DB_RETENTION_HOURS);

    AUDIT_DB_USERNAME = config.getString(NamedConstants.AUDIT_DB_USERNAME);
    AUDIT_DB_PASSWORD = config.getString(NamedConstants.AUDIT_DB_PASSWORD);
    AUDIT_DB_JDBC_URL = config.getString(NamedConstants.AUDIT_DB_JDBC_URL);
    AUDIT_DB_DATA_TABLE_NAME = config.getString(NamedConstants.AUDIT_DB_DATA_TABLE_NAME);
    AUDIT_DB_OFFSET_TABLE_NAME = config.getString(NamedConstants.AUDIT_DB_OFFSET_TABLE_NAME);
    AUDIT_DB_REMOVE_OLD_RECORD = config.getBoolean(NamedConstants.AUDIT_DB_REMOVE_OLD_RECORD);
    DUMMY_START_OFFSETS = config.getString(NamedConstants.DUMMY_START_OFFSETS);
  }
}
