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

import com.uber.stream.kafka.chaperone.collector.reporter.AuditReporterFactory;
import com.uber.stream.kafka.chaperone.collector.reporter.IAuditReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import kafka.utils.ZkUtils;

/**
 * Use high-level consumer to fetch JSON audit metrics from Kafka topic chaperone-audit.
 * But its offset logic is replaced by AuditReporter to ensure exactly once via DB transaction.
 */
public class KafkaIngester {
  private static final Logger logger = LoggerFactory.getLogger(KafkaIngester.class);

  private static final String consumerGroupId = AuditConfig.DATA_CENTER + "-ChaperoneIngester";

  private final List<KafkaIngesterConsumer> consumerThreads = new ArrayList<>(AuditConfig.INGESTER_NUM_WORKER_THREADS);

  private RedisMonitor redisMonitor = null;
  private KafkaMonitor kafkaMonitor = null;

  private final IAuditReporter auditReporter;

  private KafkaIngester() throws IOException {
    auditReporter = AuditReporterFactory.getAuditReporter(AuditConfig.AUDIT_REPORTER_TYPE);
  }

  private void run() {
    addShutdownHook();

    Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread thread, Throwable exception) {
        logger.error("Kafka ingester consumer {} failed with error {}", thread, exception);
        System.exit(1);
      }
    };

    auditReporter.start();

    if (AuditConfig.INGESTER_ENABLE_DEDUP) {
      redisMonitor =
          new RedisMonitor(AuditConfig.GRAPHITE_REPORT_PERIOD_SEC, AuditConfig.INGESTER_REDIS_HOST,
              AuditConfig.INGESTER_REDIS_PORT, AuditConfig.INGESTER_REDIS_MAXMEM_BYTES,
              AuditConfig.INGESTER_REDIS_MAXMEM_POLICY);
      redisMonitor.start();
    }

    kafkaMonitor =
        new KafkaMonitor(AuditConfig.GRAPHITE_REPORT_PERIOD_SEC, AuditConfig.KAFKA_BROKER_LIST,
            AuditConfig.AUDIT_TOPIC_NAME, auditReporter);
    kafkaMonitor.start();

    if (AuditConfig.INGESTER_START_FROM_TAIL) {
      // remove offset info from zk to force consumers start from latest offset, i.e tail
      logger.info("Remove offsets from zookeeper to tail topic");
      removeOffsetInfoFromZk(consumerGroupId);
    } else if (!AuditConfig.IGNORE_OFFSETS_IN_PERSISTENT_STORE) {
      Map<String, Map<Integer, Long>> topicOffsetsMap = auditReporter.getOffsetsToStart();
      if (topicOffsetsMap.isEmpty()) {
        logger.warn("Offsets table must be empty. Use offsets stored in zookeeper");
      } else {
        logger.info("Write db offsets to zookeeper to direct high level consumers. Offsets={}", topicOffsetsMap);
        putOffsetInfoIntoZk(consumerGroupId, topicOffsetsMap);
      }
    } else {
      logger.info("Use offsets stored in zookeeper");
    }

    Set<String> blacklistedTopics = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    if (AuditConfig.INGESTER_BLACKLISTED_TOPICS != null && AuditConfig.INGESTER_BLACKLISTED_TOPICS.length() > 0) {
      String[] topics = AuditConfig.INGESTER_BLACKLISTED_TOPICS.split(",");
      Collections.addAll(blacklistedTopics, topics);
    }
    logger.info("KafkaIngester blacklisted topics=" + blacklistedTopics);

    // Start the KafkaIngesterConsumer worker threads. Each thread creates its own
    // Kafka consumer to consume messages for the audit topic.
    for (int id = 0; id < AuditConfig.INGESTER_NUM_WORKER_THREADS; id++) {
      KafkaIngesterConsumer consumerThread = new KafkaIngesterConsumer(this, id, auditReporter, blacklistedTopics);
      consumerThread.setUncaughtExceptionHandler(handler);
      consumerThreads.add(consumerThread);
      consumerThread.start();
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        for (KafkaIngesterConsumer consumerThread : consumerThreads) {
          consumerThread.shutdown();
        }
        auditReporter.shutdown();
        if (redisMonitor != null) {
          redisMonitor.stop();
        }
        if (kafkaMonitor != null) {
          kafkaMonitor.stop();
        }
        logger.info("Kafka ingester shutdown successfully");
      }
    });
  }

  private static void putOffsetInfoIntoZk(String groupId, Map<String, Map<Integer, Long>> topicOffsetsMap) {
    ZkUtils zkUtils =
        ZkUtils.apply(AuditConfig.INGESTER_ZK_CONNECT, Integer.valueOf(AuditConfig.INGESTER_ZK_SESSION_TIMEOUT_MS),
            Integer.valueOf(AuditConfig.INGESTER_ZK_SESSION_TIMEOUT_MS), false);
    try {
      for (Map.Entry<String, Map<Integer, Long>> topicEntry : topicOffsetsMap.entrySet()) {
        String zkPath = String.format("%s/%s/offsets/%s/", ZkUtils.ConsumersPath(), groupId, topicEntry.getKey());
        for (Map.Entry<Integer, Long> offsetEntry : topicEntry.getValue().entrySet()) {
          logger.info("Put offset={} to partition={} with znode path={}", offsetEntry.getValue(), offsetEntry.getKey(),
              zkPath + offsetEntry.getKey());
          zkUtils.updatePersistentPath(zkPath + offsetEntry.getKey(), offsetEntry.getValue().toString(),
              zkUtils.DefaultAcls());
        }
      }
    } catch (Exception e) {
      logger.error("Got exception to put offset, with zkPathPrefix={}",
          String.format("%s/%s/offsets", ZkUtils.ConsumersPath(), groupId));
      throw e;
    } finally {
      zkUtils.close();
    }
  }

  // Assuming that offset info is stored in zk
  private static void removeOffsetInfoFromZk(final String groupId) {
    ZkUtils zkUtils =
        ZkUtils.apply(AuditConfig.INGESTER_ZK_CONNECT, Integer.valueOf(AuditConfig.INGESTER_ZK_SESSION_TIMEOUT_MS),
            Integer.valueOf(AuditConfig.INGESTER_ZK_SESSION_TIMEOUT_MS), false);
    try {
      String[] targets = new String[] {"offsets", "owners"};
      for (String target : targets) {
        String zkPath = String.format("%s/%s/%s", ZkUtils.ConsumersPath(), groupId, target);
        logger.info("Remove {} with znode path={}", target, zkPath);
        zkUtils.deletePathRecursive(zkPath);
      }
    } catch (Exception e) {
      logger.error("Got exception to remove offsets or owners from zookeeper, with zkPathPrefix={}",
          String.format("%s/%s/", ZkUtils.ConsumersPath(), groupId));
      throw e;
    } finally {
      zkUtils.close();
    }
  }

  String getConsumerGroupId() {
    return consumerGroupId;
  }

  public static void main(String[] args) throws Exception {
    logger.info("Kafka ingester topic : {} ", AuditConfig.AUDIT_TOPIC_NAME);
    AuditConfig.loadConfig();
    KafkaIngester ingester = new KafkaIngester();
    ingester.run();
  }
}
