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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.codahale.metrics.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.Jedis;

public class RedisMonitor {
  private static final Logger logger = LoggerFactory.getLogger(RedisMonitor.class);

  private final Jedis jedis;
  private final long checkIntervalInSec;
  private final ScheduledExecutorService cronExecutor;

  private final AtomicLong usedMemoryInBytes = new AtomicLong(0);
  private final AtomicLong usedMemoryRssInBytes = new AtomicLong(0);
  private final AtomicDouble memoryFragmentRatio = new AtomicDouble(0.0);
  private final AtomicLong keyCount = new AtomicLong(0);
  private final AtomicLong evictedKeyCount = new AtomicLong(0);
  private final AtomicLong expiredKeyCount = new AtomicLong(0);

  public RedisMonitor(long checkIntervalInSec, String redisHost, int redisPort, long maxMemBytes, String maxMemPolicy) {
    this.checkIntervalInSec = checkIntervalInSec;
    jedis = new Jedis(redisHost, redisPort);
    logger.info("RedisMonitor connected to Redis server: {}", jedis.ping());

    setRedisConfig("maxmemory", maxMemBytes + "");
    setRedisConfig("maxmemory-policy", maxMemPolicy);

    cronExecutor =
        Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("redis-monitor-%d").build());
  }

  private void setRedisConfig(String configName, String newValue) {
    List<String> config = jedis.configGet(configName);
    logger.info("Set redisConfig={} from oldValue={} to newValue={}", configName, config.get(1), newValue);
    String status = jedis.configSet(configName, newValue);
    Preconditions.checkState(status.equals("OK"), String.format("Set %s to %s for redis failed", configName, newValue));
  }

  public void start() {
    Metrics.getRegistry().register("redisMonitor.usedMemoryInBytes", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return usedMemoryInBytes.longValue();
      }
    });

    Metrics.getRegistry().register("redisMonitor.usedMemoryRssInBytes", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return usedMemoryRssInBytes.longValue();
      }
    });

    Metrics.getRegistry().register("redisMonitor.memoryFragmentRatio", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return memoryFragmentRatio.doubleValue();
      }
    });

    Metrics.getRegistry().register("redisMonitor.keyCount", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return keyCount.longValue();
      }
    });

    Metrics.getRegistry().register("redisMonitor.expiredKeyCount", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return expiredKeyCount.longValue();
      }
    });

    Metrics.getRegistry().register("redisMonitor.evictedKeyCount", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return evictedKeyCount.longValue();
      }
    });

    cronExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          String memoryStats = jedis.info("Memory");
          loadMetrics(memoryStats);
          String keyStats = jedis.info("Keyspace");
          loadMetrics(keyStats);
          String stats = jedis.info("Stats");
          loadMetrics(stats);
        } catch (Exception e) {
          logger.warn("RedisMonitor got exception to get info from Redis server. Reconnect next time.", e);
          // disconnect so that next time the jedis can reconnect
          jedis.disconnect();
        } catch (Throwable t) {
          logger.error("RedisMonitor got error to get info from Redis server", t);
          throw t;
        }
      }
    }, checkIntervalInSec, checkIntervalInSec, TimeUnit.SECONDS);
  }

  private void loadMetrics(String info) {
    for (String line : info.split("\r\n")) {
      final String[] cols = line.split(":", 2);
      if (cols.length == 2) {
        switch (cols[0]) {
          case "used_memory":
            usedMemoryInBytes.set(Long.parseLong(cols[1]));
            break;
          case "used_memory_rss":
            usedMemoryRssInBytes.set(Long.parseLong(cols[1]));
            break;
          case "mem_fragmentation_ratio":
            memoryFragmentRatio.set(Double.parseDouble(cols[1]));
            break;
          case "expired_keys":
            expiredKeyCount.set(Long.parseLong(cols[1]));
            break;
          case "evicted_keys":
            evictedKeyCount.set(Long.parseLong(cols[1]));
            break;
          case "db0":
            String countInStr = cols[1].substring(cols[1].indexOf('=') + 1, cols[1].indexOf(','));
            keyCount.set(Long.parseLong(countInStr));
            break;
        }
      }
    }
  }

  public void stop() {
    cronExecutor.shutdownNow();
    jedis.close();
    logger.info("RedisMonitor closed connection to Redis server");
  }
}
