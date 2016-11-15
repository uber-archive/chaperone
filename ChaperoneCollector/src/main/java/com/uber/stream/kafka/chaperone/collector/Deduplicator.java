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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.Jedis;

/**
 * Do deduplication by redis. This is shared by the consumer thread.
 */
public class Deduplicator {
  private static final Logger logger = LoggerFactory.getLogger(Deduplicator.class);

  private final Meter OLD_RECORDS_DETECTED;
  private final Meter DUP_RECORDS_DETECTED;
  private final Meter REDIS_CAS_FAILURE;
  private final Timer REDIS_CAS_LATENCY;

  private final int identity;
  private final String redisHost;
  private final int redisPort;
  private final int keyTTLInSec;
  private Jedis jedis;
  private final String dupHostPrefix;
  private final Set<String> hostSetWithDup;
  private final boolean hasDupHostPrefix;

  public Deduplicator(int id, String redisHost, int redisPort, int keyTTLInSec, String dupHostPrefix,
      String hostsWithDup) {
    this.identity = id;
    this.redisHost = redisHost;
    this.redisPort = redisPort;
    this.keyTTLInSec = keyTTLInSec;

    this.hasDupHostPrefix = !StringUtils.isEmpty(dupHostPrefix);
    this.dupHostPrefix = dupHostPrefix;
    String[] hosts = hostsWithDup.split(",");
    this.hostSetWithDup = new HashSet<>();
    Collections.addAll(hostSetWithDup, hosts);
    logger.info("Hosts that might send out duplicate msg: {} and dupHostPrefix={}", hostSetWithDup, dupHostPrefix);

    OLD_RECORDS_DETECTED = Metrics.getRegistry().meter(String.format("deduplicator.%d.oldRecordsDetected", identity));
    DUP_RECORDS_DETECTED = Metrics.getRegistry().meter(String.format("deduplicator.%d.dupRecordsDetected", identity));
    REDIS_CAS_FAILURE = Metrics.getRegistry().meter(String.format("deduplicator.%d.redisCheckAndSetFailure", identity));
    REDIS_CAS_LATENCY = Metrics.getRegistry().timer(String.format("deduplicator.%d.redisCheckAndSetLatency", identity));
  }

  public void open() {
    jedis = new Jedis(redisHost, redisPort);
    logger.info("Deduplicator={} connected to Redis server host={}, port={}, ping={}", identity, redisHost, redisPort,
        jedis.ping());
  }

  // for test purpose
  public void open(Jedis jedis) {
    this.jedis = jedis;
    logger.info("Deduplicator={} connected to Redis server host={}, port={}, ping={}", identity, "testMode", -1,
        jedis.ping());
  }

  public void close() {
    jedis.close();
    logger.info("Deduplicator={} closed connection to Redis server", identity);
  }

  /**
   *
   * @return true if put; false if duplicate one exists.
   * @param topicName the AuditMsg is generated for
   * @param partitionId (partitionId, offset) tells where the AuditMsg is in the auditmsg topic
   * @param offset
   * @param host as a filter. not all hosts can send out duplicate msg.
   * @param uuid
   */
  public boolean isDuplicated(String topicName, int partitionId, long offset, String host, String uuid)
      throws InterruptedException {
    if (maybeDuplicate(host)) {
      // topicName, partitionId, offset, uuid ID a msg uniquely. host as a filter
      logger.debug("Deduplicator={} checking msg topic={}, partitionId={}, offset={}, host={}, uuid={}", identity,
          topicName, partitionId, offset, host, uuid);

      int retry = 3;
      while (retry > 0) {
        Timer.Context ctx = REDIS_CAS_LATENCY.time();
        try {
          String partitionIdOffsetInMap = jedis.get(uuid);
          String curPartitionIdOffset = partitionId + "_" + offset;
          if (!StringUtils.isEmpty(partitionIdOffsetInMap) && !partitionIdOffsetInMap.equals("OK")) {
            // on closing, OK is returned instead of value for the key
            if (!partitionIdOffsetInMap.equals(curPartitionIdOffset)) {
              logger.info("Deduplicator={} gets DUP msg with uuid={}, curPartOffset={}, partOffsetInMap={}", identity,
                  uuid, curPartitionIdOffset, partitionIdOffsetInMap);
              DUP_RECORDS_DETECTED.mark();
              return true;
            } else {
              // re-fetched msg. This is not treated as duplicate. The msg was just not processed
              // during last run
              logger.debug("Deduplicator={} gets OLD msg with uuid={}, curPartOffset={}, partOffsetInMap={}", identity,
                  uuid, curPartitionIdOffset, partitionIdOffsetInMap);
              OLD_RECORDS_DETECTED.mark();
            }
          } else {
            logger.debug("Deduplicator={} gets NEW msg with uuid={}, curPartOffset={}, partOffsetInMap={}", identity,
                uuid, curPartitionIdOffset, partitionIdOffsetInMap);
            jedis.setex(uuid, keyTTLInSec, curPartitionIdOffset);
          }
          break;
        } catch (Exception e) {
          logger.warn(
              String.format("Deduplicator=%d got exception to access redis. RetryLeft=%d", identity, (--retry)), e);
          REDIS_CAS_FAILURE.mark();
          // disconnect cleanup internal objects and can trigger reconnect on next request
          jedis.disconnect();
          Thread.sleep(500);
        } finally {
          ctx.stop();
        }
      }
    }

    return false;
  }

  private boolean maybeDuplicate(String host) {
    return hostSetWithDup.contains(host) || (hasDupHostPrefix && host.startsWith(dupHostPrefix));
  }
}
