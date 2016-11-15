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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.codahale.metrics.Gauge;
import com.uber.stream.kafka.chaperone.collector.reporter.IAuditReporter;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 * The high level consumer only has MaxLag metrics, which is not enough.
 * Use SimpleConsumer to get offset lag info for all partitions.
 */
public class KafkaMonitor {

  private static final Logger logger = LoggerFactory.getLogger(KafkaMonitor.class);
  private static final String OFFSET_LAG_NAME_FORMAT = "kafkaMonitor.%s.%d.offsetLag";

  private final Map<String, SimpleConsumer> brokerConsumer;

  private final Map<TopicAndPartition, String> partitionLeader;
  private final Map<TopicAndPartition, Long> partitionLag;

  private final List<String> brokerList;
  private final List<String> auditTopics;
  private final IAuditReporter auditReporter;

  // offset lag metrics as gauge
  private final Set<TopicAndPartition> partitionInjected;

  private final ScheduledExecutorService cronExecutor;
  private final long checkIntervalInSec;

  public KafkaMonitor(long checkIntervalInSec, String brokerList, String auditTopics, IAuditReporter auditReporter) {
    this.checkIntervalInSec = checkIntervalInSec;
    this.brokerList = Arrays.asList(StringUtils.split(brokerList, ","));
    this.auditTopics = Arrays.asList(StringUtils.split(auditTopics, ","));
    this.auditReporter = auditReporter;
    this.brokerConsumer = new HashMap<>();
    this.partitionLeader = new HashMap<>();
    this.partitionLag = new ConcurrentHashMap<>();
    this.partitionInjected = new HashSet<>();

    cronExecutor =
        Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("kafka-monitor-%d").build());
  }

  public void start() {
    logger.info("KafkaMonitor starts with brokerList=" + brokerList);
    updateLeaderMap();
    cronExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        updateLatestOffset();
      }
    }, checkIntervalInSec / 2, checkIntervalInSec, TimeUnit.SECONDS);
  }

  public void stop() {
    cronExecutor.shutdownNow();
    for (SimpleConsumer consumer : brokerConsumer.values()) {
      consumer.close();
    }
    logger.info("KafkaMonitor closed connection to brokers");
  }

  private void updateLatestOffset() {
    logger.info("KafkaMonitor updates latest offset with leaders=" + partitionLeader);

    boolean needUpdateLeaderMap = false;
    Map<TopicAndPartition, Long> partitionLatestOffset = new HashMap<>();
    for (Map.Entry<TopicAndPartition, String> entry : partitionLeader.entrySet()) {
      String leaderBroker = entry.getValue();
      TopicAndPartition tp = entry.getKey();
      if (StringUtils.isEmpty(leaderBroker)) {
        logger.warn("{} does not have leader partition", tp);
        needUpdateLeaderMap = true;
      } else {
        try {
          SimpleConsumer consumer = getSimpleConsumer(leaderBroker);
          long latestOffset = getLatestOffset(consumer, tp);
          if (latestOffset < 0) {
            needUpdateLeaderMap = true;
          }
          partitionLatestOffset.put(tp, latestOffset);
          logger.debug("Get latest offset={} for {}", latestOffset, tp);
        } catch (Exception e) {
          logger.warn("Got exception to get offset for TopicPartition=" + tp, e);
          needUpdateLeaderMap = true;
        }
      }
    }

    Map<String, Map<Integer, Long>> commitOffsetsMap = auditReporter.getOffsetsToStart();
    if (commitOffsetsMap.isEmpty()) {
      logger.warn("There is no commit offsets info");
    } else {
      for (Map.Entry<String, Map<Integer, Long>> topicEntry : commitOffsetsMap.entrySet()) {
        String topicName = topicEntry.getKey();
        for (Map.Entry<Integer, Long> offsetEntry : topicEntry.getValue().entrySet()) {
          long commitOffset = offsetEntry.getValue();
          TopicAndPartition tp = new TopicAndPartition(topicName, offsetEntry.getKey());
          Long latestOffset = partitionLatestOffset.get(tp);
          if (latestOffset != null) {
            long lag = latestOffset - commitOffset;
            partitionLag.put(tp, lag);

            injectMetrics(tp);

            logger.info("{} has latest={}, commit={}, lag={}", tp, latestOffset, commitOffset, lag);
          } else {
            logger.warn("{} does not have latestOffset info but commit={}", tp, commitOffset);
          }
        }
      }
    }

    if (partitionLeader.isEmpty() || needUpdateLeaderMap) {
      updateLeaderMap();
    }
  }

  private void updateLeaderMap() {
    for (String broker : brokerList) {
      try {
        SimpleConsumer consumer = getSimpleConsumer(broker);
        TopicMetadataRequest req = new TopicMetadataRequest(auditTopics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        List<TopicMetadata> metaData = resp.topicsMetadata();

        for (TopicMetadata tmd : metaData) {
          for (PartitionMetadata pmd : tmd.partitionsMetadata()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(tmd.topic(), pmd.partitionId());
            partitionLeader.put(topicAndPartition, getHostPort(pmd.leader()));
          }
        }
      } catch (Exception e) {
        logger.warn("Got exception to get metadata from broker=" + broker, e);
      }
    }
  }

  private void injectMetrics(final TopicAndPartition topicAndPartition) {
    if (!partitionInjected.contains(topicAndPartition)) {
      Metrics.getRegistry().register(
          String.format(OFFSET_LAG_NAME_FORMAT, topicAndPartition.topic(), topicAndPartition.partition()),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              if (partitionLag.containsKey(topicAndPartition)) {
                return partitionLag.get(topicAndPartition);
              } else {
                return -1L;
              }
            }
          });
      partitionInjected.add(topicAndPartition);
    }
  }

  private static String getHostPort(BrokerEndPoint leader) {
    if (leader != null) {
      return leader.host() + ":" + leader.port();
    }
    return null;
  }

  private SimpleConsumer getSimpleConsumer(String broker) {
    SimpleConsumer consumer = brokerConsumer.get(broker);
    if (consumer == null) {
      int idx = broker.indexOf(":");
      if (idx >= 0) {
        String brokerHost = broker.substring(0, idx);
        String port = broker.substring(idx + 1);
        consumer =
            new SimpleConsumer(brokerHost, Integer.parseInt(port), 60000, 64 * 1024, "metadataFetcher-" + brokerHost);
        brokerConsumer.put(broker, consumer);
      }
    }
    return consumer;
  }

  private static long getLatestOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
    kafka.javaapi.OffsetRequest request =
        new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      logger.warn("Failed to fetch offset for {} due to {}", topicAndPartition,
          response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
      return -1;
    }

    long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
    return offsets[0];
  }
}
