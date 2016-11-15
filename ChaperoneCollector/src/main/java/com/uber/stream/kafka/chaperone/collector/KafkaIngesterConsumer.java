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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxAttributeGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.uber.stream.kafka.chaperone.collector.reporter.IAuditReporter;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * KafkaIngesterConsumer is a worker thread that creates one Kafka ConsumerConnector
 * with one stream and continuously consumes from audit topic. We keep partition/offset
 * information with each msg when storing them in database to ensure 'exactly once'.
 */
public class KafkaIngesterConsumer extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(KafkaIngesterConsumer.class);
  private final KafkaIngester kafkaIngester;

  private ConsumerConnector consumer;
  private ConsumerIterator<byte[], byte[]> iterator;

  private CountDownLatch shutdownComplete = new CountDownLatch(1);

  // Gauge extract value from MBeans (kafka.consumer, ConsumerFetcherManager,
  // MaxLag) to report max kafka offset lag at per consumer level
  private Gauge kafkaOffsetLagGauge;
  private static final String maxLagMetricName = "\"kafka.consumer\":type=\"ConsumerFetcherManager\",name=\""
      + AuditConfig.DATA_CENTER + "-ChaperoneIngester-MaxLag\"";

  private Meter failedToIngestCounter;
  private Meter fetchedMsgCounter;

  private final IAuditReporter auditReporter;
  private final boolean enablePersistentStore;
  private final Set<String> blacklistedTopics;
  private final int threadId;
  private Deduplicator deduplicator;

  public KafkaIngesterConsumer(KafkaIngester kafkaIngester, int threadId, IAuditReporter auditReporter,
      Set<String> blacklistedTopics) {
    this.kafkaIngester = kafkaIngester;
    this.setName("KafkaIngesterConsumer-" + threadId);
    this.auditReporter = auditReporter;
    this.enablePersistentStore = AuditConfig.ENABLE_PERSISTENT_STORE;
    this.blacklistedTopics = blacklistedTopics;
    this.threadId = threadId;
  }

  private void init() {
    // register kafka offset lag metrics, one Gauge is for per consumer level granularity
    MetricRegistry registry = Metrics.getRegistry();
    try {
      fetchedMsgCounter = registry.meter("kafkaIngesterConsumer." + this.getName() + "-msgFetchRate");
      failedToIngestCounter = registry.meter("kafkaIngesterConsumer." + this.getName() + "-failedToIngest");
      kafkaOffsetLagGauge =
          registry.register("kafkaIngesterConsumer." + this.getName() + "-kafkaOffsetLag", new JmxAttributeGauge(
              new ObjectName(maxLagMetricName), "Value"));
    } catch (MalformedObjectNameException | IllegalArgumentException e) {
      logger.error("Register failure for metrics of KafkaIngesterConsumer", e);
    }

    TopicFilter topicFilter = new Whitelist(AuditConfig.AUDIT_TOPIC_NAME);
    logger.info("{}: Topic filter is {}", getName(), AuditConfig.AUDIT_TOPIC_NAME);
    this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
    KafkaStream<byte[], byte[]> stream = consumer.createMessageStreamsByFilter(topicFilter, 1).get(0);
    iterator = stream.iterator();
    logger.info("KafkaIngesterConsumer thread {} is initialized successfully", getName());

    if (AuditConfig.INGESTER_ENABLE_DEDUP) {
      deduplicator =
          new Deduplicator(threadId, AuditConfig.INGESTER_REDIS_HOST, AuditConfig.INGESTER_REDIS_PORT,
              AuditConfig.INGESTER_REDIS_KEY_TTL_SEC, AuditConfig.INGESTER_DUP_HOST_PREFIX,
              AuditConfig.INGESTER_HOSTS_WITH_DUP);
      deduplicator.open();
    } else {
      deduplicator = null;
    }
  }

  /**
   * Runs a continuous loop to consume messages over a Kafka Consumer stream.
   * It breaks out when the consumer has been shutdown and there are no more messages.
   */
  @Override
  public void run() {
    try {
      // Set up the consumer stream and the iterators.
      // Not doing in the constructor so that it can happen in parallel
      init();

      while (iterator.hasNext()) {
        fetchedMsgCounter.mark();
        MessageAndMetadata mm = null;
        try {
          mm = iterator.next();
          processAuditMsg(mm);
        } catch (Exception e) {
          failedToIngestCounter.mark();
          logger.error("Got exception to iterate/process msg", e);
        }
      }
      logger.info("KafkaIngesterConsumer {} exiting", getName());
    } finally {
      shutdownComplete.countDown();
    }
  }

  private void processAuditMsg(final MessageAndMetadata mm) throws Exception {
    JSONObject record = JSON.parseObject(StringUtils.toEncodedString((byte[]) mm.message(), Charset.forName("UTF-8")));

    String topicName = record.getString(AuditMsgField.TOPICNAME.getName());
    if (blacklistedTopics.contains(topicName)) {
      logger.debug("Topic={} is blacklisted", topicName);
      return;
    }

    if (deduplicator != null) {
      String uuid = record.getString(AuditMsgField.UUID.getName());
      String host = record.getString(AuditMsgField.HOSTNAME.getName());
      if (deduplicator.isDuplicated(topicName, mm.partition(), mm.offset(), host, uuid)) {
        return;
      }
    }

    if (enablePersistentStore) {
      auditReporter.submit(mm.topic(), mm.partition(), mm.offset(), record);
    }
  }

  /**
   * Shutdown the KafkaIngesterConsumer.
   */
  public void shutdown() {
    logger.info("Consumer thread {} shutting down", getName());
    try {
      consumer.shutdown();
      if (deduplicator != null) {
        deduplicator.close();
      }
      // Wait for consumer stream to drain and run loop to end.
      Thread.sleep(200);
      if (shutdownComplete.getCount() == 1) {
        // This is to avoid any deadlock or hangs in consumer shutdown.
        interrupt();
      }
      shutdownComplete.await();
    } catch (InterruptedException ie) {
      logger.debug("Shutdown for {} was interrupted", getName());
    } catch (Exception e) {
      logger.warn("Exception when shutting down KafkaIngesterConsumer thread {} {}", getName(), e);
    }
    logger.info("Consumer thread {} successfully shut down", getName());
  }

  /**
   * Create a consumer configuration with all the configured static & runtime
   * properties.
   *
   * @return ConsumerConfig object.
   */
  private ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", AuditConfig.INGESTER_ZK_CONNECT);
    props.put("group.id", kafkaIngester.getConsumerGroupId());
    props.put("zookeeper.session.timeout.ms", AuditConfig.INGESTER_ZK_SESSION_TIMEOUT_MS);
    props.put("zookeeper.sync.time.ms", AuditConfig.INGESTER_ZK_SYNC_TIME_MS);
    props.put("auto.offset.reset", AuditConfig.INGESTER_START_FROM_TAIL ? "largest"
        : AuditConfig.INGESTER_AUTO_OFFSET_RESET);
    // Offsets are managed by IAuditReporter that puts offsets in DB along with audit msg
    props.put("auto.commit.enable", AuditConfig.INGESTER_AUTO_COMMIT_ENABLE);
    props.put("auto.commit.interval.ms", AuditConfig.INGESTER_AUTO_COMMIT_INTERVAL_MS);
    return new ConsumerConfig(props);
  }
}
