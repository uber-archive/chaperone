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

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metrics utility class based on dropwizard metrics library to manage the
 * registry and report metrics to JMX and Graphite.
 */
public class Metrics {
  private static final MetricRegistry registry = new MetricRegistry();
  public static JmxReporter reporter;
  private static final AtomicBoolean inited = new AtomicBoolean(false);
  private static String PREFIX = String.format("stats.%s.chaperoneingester", AuditConfig.DATA_CENTER);

  private static void init() {
    // Init JMX reporter
    reporter = JmxReporter.forRegistry(registry).build();
    reporter.start();
    // Init graphite reporter
    Graphite graphite = getGraphite();
    GraphiteReporter graphiteReporter;
    if (graphite == null) {
      graphiteReporter = null;
    } else {
      graphiteReporter =
          GraphiteReporter.forRegistry(registry).prefixedWith(PREFIX).convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL).build(graphite);
      graphiteReporter.start(AuditConfig.GRAPHITE_REPORT_PERIOD_SEC, TimeUnit.SECONDS);
    }
  }

  public static MetricRegistry getRegistry() {
    if (inited.compareAndSet(false, true)) {
      init();
    }
    return registry;
  }

  private static Graphite getGraphite() {
    if (AuditConfig.GRAPHITE_HOST_NAME == null || AuditConfig.GRAPHITE_PORT == 0
        || AuditConfig.GRAPHITE_REPORT_PERIOD_SEC == 0L) {
      return null;
    }
    InetSocketAddress graphiteAddress =
        new InetSocketAddress(AuditConfig.GRAPHITE_HOST_NAME, AuditConfig.GRAPHITE_PORT);
    return new Graphite(graphiteAddress);
  }
}
