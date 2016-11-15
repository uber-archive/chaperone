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

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

/**
 * Class to report auditing message to configured Kafka
 */
public class KafkaAuditReporter implements AuditReporter {
  private static final Logger logger = LoggerFactory.getLogger(KafkaAuditReporter.class);

  private final String topicForAuditMsg;
  private Producer<String, byte[]> producer;
  private HostMetadata hostMetadata;

  private final Meter messageReportRate;
  private final Meter bucketReportRate;

  private Producer<String, byte[]> getKafkaProducer(String brokerList, String requiredAcks, HostMetadata host) {
    Properties properties = new Properties();
    // Old props list
    properties.put("metadata.broker.list", brokerList);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("key.serializer.class", "kafka.serializer.StringEncoder");
    properties.put("compression.type", "gzip");
    properties.put("request.required.acks", requiredAcks);
    // New props needed
    properties.put("bootstrap.servers", brokerList);
    properties.put("client.id", host.getTier() + "-" + host.getHost());
    return new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
  }

  public KafkaAuditReporter(String topicForAuditMsg, String auditDC, String auditTier, String brokerList,
      String requiredAcks, MetricRegistry metricRegistry) throws IOException {
    this.topicForAuditMsg = topicForAuditMsg;
    this.hostMetadata = new HostMetadata(auditDC, auditTier);
    this.producer = getKafkaProducer(brokerList, requiredAcks, hostMetadata);

    if (metricRegistry == null) {
      logger.info("Using temporary metrics registry");
      metricRegistry = new MetricRegistry();
    }
    this.messageReportRate = metricRegistry.meter("kafkaAuditReporter.messageReportRate");
    this.bucketReportRate = metricRegistry.meter("kafkaAuditReporter.bucketReportRate");
  }

  public AuditMessage buildAuditMessage(String topicName, TimeBucketMetadata timeBucket) {
    return new AuditMessage(topicName, timeBucket, hostMetadata);
  }

  public Meter getMessageReportRate() {
    return messageReportRate;
  }

  public Meter getBucketReportRate() {
    return bucketReportRate;
  }

  public void shutdown() {
    producer.close();
    logger.info("KafkaReporter was shut down successfully");
  }

  public void reportAuditMessage(final AuditMessage message) throws IOException {
    // Create a heatpipe-encoded message.
    final JSONObject auditMsg = new JSONObject();
    auditMsg.put(AuditMsgField.TOPICNAME.getName(), message.topicName);
    auditMsg.put(AuditMsgField.TIME_BUCKET_START.getName(), message.timeBucketMetadata.timeBucketStartInSec);
    auditMsg.put(AuditMsgField.TIME_BUCKET_END.getName(), message.timeBucketMetadata.timeBucketEndInSec);
    auditMsg.put(AuditMsgField.METRICS_COUNT.getName(), message.timeBucketMetadata.msgCount.get());
    auditMsg.put(AuditMsgField.METRICS_MEAN_LATENCY.getName(), message.timeBucketMetadata.latencyStats.getMean());
    auditMsg
        .put(AuditMsgField.METRICS_P95_LATENCY.getName(), message.timeBucketMetadata.latencyStats.getPercentile(95));
    auditMsg
        .put(AuditMsgField.METRICS_P99_LATENCY.getName(), message.timeBucketMetadata.latencyStats.getPercentile(99));
    auditMsg.put(AuditMsgField.METRICS_MAX_LATENCY.getName(), message.timeBucketMetadata.latencyStats.getMax());
    auditMsg.put(AuditMsgField.TIER.getName(), message.hostMetadata.getTier());
    auditMsg.put(AuditMsgField.HOSTNAME.getName(), message.hostMetadata.getHost());
    auditMsg.put(AuditMsgField.DATACENTER.getName(), message.hostMetadata.getDatacenter());
    auditMsg.put(AuditMsgField.UUID.getName(), UUID.randomUUID().toString());

    final byte[] outputBytes = auditMsg.toJSONString().getBytes();
    // Send message via Kafka producer
    final ProducerRecord<String, byte[]> data = new ProducerRecord<>(topicForAuditMsg, null, outputBytes);

    producer.send(data, new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
          logger.warn("Could not send auditMsg over Kafka for topic {}", message.topicName, e);
        } else {
          messageReportRate.mark(message.timeBucketMetadata.msgCount.get());
          bucketReportRate.mark();
        }
      }
    });
  }
}
