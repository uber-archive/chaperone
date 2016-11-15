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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.uber.stream.kafka.chaperone.collector.Metrics;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Store the metrics to database used by chaperone_app directly.
 * The pass/user can be found on mupass. But be cautious to put pass/user in code/config.
 */
class DbAuditReporter extends AuditReporter {
  private static final Logger logger = LoggerFactory.getLogger(DbAuditReporter.class);
  private final BasicDataSource ds;
  private final ScheduledExecutorService cronExecutor;

  private final String dataTableName;
  private final String offsetTableName;

  private static final String CREATE_DATA_TABLE_SQL =
      "CREATE TABLE IF NOT EXISTS %s (time_begin DATETIME, time_end DATETIME, topic_name VARCHAR(128), "
          + "host_name VARCHAR(64), audit_dc VARCHAR(64), tier VARCHAR(64), metrics text, "
          + "PRIMARY KEY (time_begin, time_end, topic_name, host_name, audit_dc, tier))";

  private static final String CREATE_OFFSET_TABLE_SQL =
      "CREATE TABLE IF NOT EXISTS %s (topic_name VARCHAR(128), partition_id INT, offset BIGINT, "
          + "PRIMARY KEY (topic_name, partition_id))";

  private static final String GET_OFFSETS_SQL = "SELECT topic_name, partition_id, offset FROM %s ";

  private static final String INSERT_OFFSET_SQL = "INSERT INTO %s (topic_name, partition_id, offset) VALUES (?, ?, ?) "
      + "ON DUPLICATE KEY UPDATE offset=?";

  private static final String SELECT_METRICS_SQL = "SELECT metrics FROM %s "
      + "WHERE time_begin=? AND time_end=? AND topic_name=? AND host_name=? AND audit_dc=? AND tier=?";

  private static final String UPDATE_METRICS_SQL = "UPDATE %s SET metrics = ?"
      + "WHERE time_begin=? AND time_end=? AND topic_name=? AND host_name=? AND audit_dc=? AND tier=?";

  private static final String INSERT_METRICS_SQL = "INSERT INTO %s "
      + "(time_begin, time_end, topic_name, metrics, host_name, audit_dc, tier) VALUES (?, ?, ?, ?, ?, ?, ?)";

  private static final String DELETE_METRICS_SQL = "DELETE FROM %s WHERE time_begin < ?";

  private final Meter REMOVED_RECORDS_COUNTER;
  private final Meter FAILED_TO_REMOVE_COUNTER;

  private final Meter INSERTED_RECORDS_COUNTER;
  private final Meter UPDATED_RECORDS_COUNTER;

  private final Timer DB_REPORT_LATENCY_TIMER;

  private volatile long latestTSSeenLastInsert = 0;
  private volatile long earliestTSSeenLastInsert = System.currentTimeMillis();

  private final long auditDbRetentionMs;
  private final boolean enableRemoveOldRecord;

  static DbAuditReporter2Builder newBuilder() {
    return new DbAuditReporter2Builder();
  }

  static class DbAuditReporter2Builder extends AuditReporterBuilder {
    private String dbUser;
    private String dbPass;
    private String dbUrl;
    private String dataTableName;
    private String offsetTableName;
    private int dbRetentionInHr;
    private boolean enableRemoveOldRecord;

    DbAuditReporter2Builder setDbUser(String dbUser) {
      this.dbUser = dbUser;
      return this;
    }

    DbAuditReporter2Builder setDbPass(String dbPass) {
      this.dbPass = dbPass;
      return this;
    }

    DbAuditReporter2Builder setDbUrl(String dbUrl) {
      this.dbUrl = dbUrl;
      return this;
    }

    DbAuditReporter2Builder setDataTableName(String dataTableName) {
      this.dataTableName = dataTableName;
      return this;
    }

    DbAuditReporter2Builder setOffsetTableName(String offsetTableName) {
      this.offsetTableName = offsetTableName;
      return this;
    }

    DbAuditReporter2Builder setDbRetentionInHr(int dbRetentionInHr) {
      this.dbRetentionInHr = dbRetentionInHr;
      return this;
    }

    DbAuditReporter2Builder setEnableRemoveOldRecord(boolean enableRemoveOldRecord) {
      this.enableRemoveOldRecord = enableRemoveOldRecord;
      return this;
    }

    public IAuditReporter build() {
      return new DbAuditReporter(queueSize, timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec,
          combineMetricsAmongHosts, dbUser, dbPass, dbUrl, dataTableName, offsetTableName, dbRetentionInHr,
          enableRemoveOldRecord);
    }
  }

  private DbAuditReporter(int queueSize, long timeBucketIntervalInSec, int reportFreqMsgCount,
      int reportFreqIntervalSec, boolean combineMetricsAmongHosts, String dbUser, String dbPass, String dbUrl,
      String dataTableName, String offsetTableName, int dbRetentionInHr, boolean enableRemoveOldRecord) {
    super(queueSize, timeBucketIntervalInSec, reportFreqMsgCount, reportFreqIntervalSec, combineMetricsAmongHosts);

    ds = new BasicDataSource();
    ds.setDriverClassName("com.mysql.jdbc.Driver");
    ds.setUsername(dbUser);
    ds.setPassword(dbPass);
    ds.setUrl(dbUrl);

    REMOVED_RECORDS_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.removedRecordsNumber");
    INSERTED_RECORDS_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.insertedRecordsNumber");
    UPDATED_RECORDS_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.updatedRecordsNumber");
    FAILED_TO_REMOVE_COUNTER = Metrics.getRegistry().meter(getType() + ".auditReporter.failedToRemoveNumber");
    DB_REPORT_LATENCY_TIMER = Metrics.getRegistry().timer(getType() + ".auditReporter.dbReportLatencyMs");

    Metrics.getRegistry().register(getType() + ".auditReporter.latestTSSeenLastInsert", new Gauge<Long>() {
      @Override
      public Long getValue() {
        long ret = latestTSSeenLastInsert;
        latestTSSeenLastInsert = 0;
        return ret;
      }
    });
    Metrics.getRegistry().register(getType() + ".auditReporter.earliestTSSeenLastInsert", new Gauge<Long>() {
      @Override
      public Long getValue() {
        long ret = earliestTSSeenLastInsert;
        earliestTSSeenLastInsert = System.currentTimeMillis();
        return ret;
      }
    });

    cronExecutor =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(
            getType() + "-cron-executor-%d").build());

    auditDbRetentionMs = TimeUnit.HOURS.toMillis(dbRetentionInHr);
    this.dataTableName = dataTableName;
    this.offsetTableName = offsetTableName;
    this.enableRemoveOldRecord = enableRemoveOldRecord;

    logger.info("Try to create dataTable={} and offsetTable={}", dataTableName, offsetTableName);
    maybeCreateTable(CREATE_DATA_TABLE_SQL, dataTableName);
    maybeCreateTable(CREATE_OFFSET_TABLE_SQL, offsetTableName);
  }

  private void maybeCreateTable(String sql, String tblName) {
    Connection conn = null;
    PreparedStatement stmt = null;
    try {
      conn = getConnection();
      conn.setAutoCommit(true);
      stmt = conn.prepareStatement(String.format(sql, tblName));
      stmt.executeUpdate();
    } catch (Exception e) {
      logger.warn("Got exception to create table={} with sql={}", tblName, sql, e);
      rollback(conn);
    } finally {
      closeDbResource(null, stmt, conn);
    }
  }

  public Map<String, Map<Integer, Long>> getOffsetsToStart() {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    Map<String, Map<Integer, Long>> topicOffsetsMap = new HashMap<>();
    try {
      conn = getConnection();
      stmt = conn.prepareStatement(String.format(GET_OFFSETS_SQL, offsetTableName));
      rs = stmt.executeQuery();
      while (rs.next()) {
        String topicName = rs.getString("topic_name");
        Map<Integer, Long> offsets = topicOffsetsMap.get(topicName);
        if (offsets == null) {
          offsets = new HashMap<>();
          topicOffsetsMap.put(topicName, offsets);
        }
        offsets.put(rs.getInt("partition_id"), rs.getLong("offset"));
      }
    } catch (SQLException e) {
      logger.warn("Could not get offsets from database", e);
      topicOffsetsMap.clear();
    } finally {
      closeDbResource(rs, stmt, conn);
    }
    return topicOffsetsMap;
  }

  public void start() {
    super.start();

    if (enableRemoveOldRecord) {
      logger.info("Remove old records every 30min");
      cronExecutor.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          removeOldRecord();
        }
      }, 10, 30, TimeUnit.MINUTES);
    } else {
      logger.info("Keep records forever");
    }
  }

  public void shutdown() {
    super.shutdown();

    cronExecutor.shutdownNow();

    try {
      ds.close();
    } catch (SQLException e) {
      logger.warn("Got exception to close db datasource", e);
    }
  }

  public int removeOldRecord() {
    Connection conn = null;
    PreparedStatement stmt = null;
    int affected = 0;
    long ts = System.currentTimeMillis() - auditDbRetentionMs;
    try {
      logger.info("Start to remove old records per timestamp={} with retentionMs={}", ts, auditDbRetentionMs);

      conn = getConnection();
      conn.setAutoCommit(false);
      stmt = conn.prepareStatement(String.format(DELETE_METRICS_SQL, dataTableName));

      Timestamp dbTs = new Timestamp(ts);
      stmt.setTimestamp(1, dbTs);
      affected = stmt.executeUpdate();
      conn.commit();

      REMOVED_RECORDS_COUNTER.mark(affected);
      logger
          .info("Removed count={} old records per timestamp={} with retentionMs={}", affected, ts, auditDbRetentionMs);
    } catch (Exception e) {
      logger.warn("Got exception to remove old records", e);
      FAILED_TO_REMOVE_COUNTER.mark();
      rollback(conn);
    } finally {
      closeDbResource(null, stmt, conn);
    }

    return affected;
  }

  @Override
  public void report(String sourceTopic, int recordPartition, long recordOffset, JSONObject record)
      throws InterruptedException {
    if (!aggregator.addRecord(sourceTopic, recordPartition, recordOffset, record)) {
      return;
    }

    // aggregator aggregates is the fast path. only account for database report latency
    final Timer.Context timerCtx = DB_REPORT_LATENCY_TIMER.time();
    try {
      Map<Long, Map<String, TimeBucket>> buffer = aggregator.getAndResetBuffer();
      Map<String, Map<Integer, Long>> topicOffsetsMap = aggregator.getOffsets();
      logger.debug("Reporting the buffered auditMsgs={} and offsets={}", buffer, topicOffsetsMap);

      int retryTimes = 1;
      // retry until done successfully, backpressure is imposed to kafka consumers via Disruptor.
      while (true) {
        Connection conn = null;
        PreparedStatement insertMetricsStmt = null;
        PreparedStatement selectMetricsStmt = null;
        PreparedStatement updateMetricsStmt = null;
        PreparedStatement offsetInsertStmt = null;

        try {
          conn = getConnection();
          conn.setAutoCommit(false);

          insertMetricsStmt = conn.prepareStatement(String.format(INSERT_METRICS_SQL, dataTableName));
          selectMetricsStmt = conn.prepareStatement(String.format(SELECT_METRICS_SQL, dataTableName));
          updateMetricsStmt = conn.prepareStatement(String.format(UPDATE_METRICS_SQL, dataTableName));

          offsetInsertStmt = conn.prepareStatement(String.format(INSERT_OFFSET_SQL, offsetTableName));

          addOrUpdateOffsets(offsetInsertStmt, topicOffsetsMap);

          addOrUpdateRecord(selectMetricsStmt, updateMetricsStmt, insertMetricsStmt, buffer);

          conn.commit();
          return;
        } catch (Exception e) {
          int sleepInMs = Math.max(500, Math.min(60000, retryTimes * 500));
          logger.warn(String.format("Got exception to insert buckets=%d, retryTimes=%d, sleepInMs=%d", buffer.size(),
              retryTimes++, sleepInMs), e);
          int count = 0;
          for (Map<String, TimeBucket> buckets : buffer.values()) {
            count += buckets.size();
          }
          FAILED_TO_REPORT_COUNTER.mark(count);
          rollback(conn);
          Thread.sleep(sleepInMs);
        } finally {
          closeStatement(offsetInsertStmt);
          closeStatement(insertMetricsStmt);
          closeStatement(updateMetricsStmt);
          closeStatement(selectMetricsStmt);
          closeConnection(conn);
        }
      }
    } finally {
      timerCtx.stop();
    }
  }

  private void addOrUpdateOffsets(PreparedStatement offsetInsertStmt, Map<String, Map<Integer, Long>> topicOffsetsMap)
      throws SQLException {
    logger.info("Newest offset info={}", topicOffsetsMap);

    for (Map.Entry<String, Map<Integer, Long>> topicEntry : topicOffsetsMap.entrySet()) {
      String topicName = topicEntry.getKey();
      for (Map.Entry<Integer, Long> offsetEntry : topicEntry.getValue().entrySet()) {
        offsetInsertStmt.setString(1, topicName);
        offsetInsertStmt.setInt(2, offsetEntry.getKey());
        offsetInsertStmt.setLong(3, offsetEntry.getValue());
        offsetInsertStmt.setLong(4, offsetEntry.getValue());
        offsetInsertStmt.executeUpdate();
      }
    }
  }

  /**
   * metrics is JSON string, that cannot use 'INSERT ... ON DUPLICATE KEY UPDATE'
   * but test shows that the rows have very good time locality, thus mysql cache helps
   * to de select-check-update operation.
   */
  private void addOrUpdateRecord(PreparedStatement selectStmt, PreparedStatement updateStmt,
      PreparedStatement insertStmt, Map<Long, Map<String, TimeBucket>> buffer) throws SQLException {
    for (Map<String, TimeBucket> buckets : buffer.values()) {
      for (TimeBucket bucket : buckets.values()) {
        if (bucket.count == 0) {
          continue;
        }

        // do select-check-update for
        long beginTsInMs = TimeUnit.SECONDS.toMillis(bucket.timeBeginInSec);
        long endTsInMs = TimeUnit.SECONDS.toMillis(bucket.timeEndInSec);

        Timestamp beginDbTs = new Timestamp(beginTsInMs);
        Timestamp endDbTs = new Timestamp(endTsInMs);

        // search by primary key, only one can return if existing
        selectStmt.setTimestamp(1, beginDbTs);
        selectStmt.setTimestamp(2, endDbTs);
        selectStmt.setString(3, bucket.topicName);
        selectStmt.setString(4, bucket.hostName);
        selectStmt.setString(5, bucket.datacenter);
        selectStmt.setString(6, bucket.tier);
        ResultSet rs = null;

        try {
          rs = selectStmt.executeQuery();
          if (rs.next()) {
            String metricsInStr = rs.getString("metrics");
            JSONObject oldMetricsInJSON = JSON.parseObject(metricsInStr);
            long count = oldMetricsInJSON.getLong("count");
            double meanLatency = oldMetricsInJSON.getDouble("mean");
            double p95Latency = oldMetricsInJSON.getDouble("mean95");

            // be back-compatible with old version AuditMsg
            long totalBytes = oldMetricsInJSON.getLongValue("totalBytes");
            long noTimeCount = oldMetricsInJSON.getLongValue("noTimeCount");
            long malformedCount = oldMetricsInJSON.getLongValue("malformedCount");
            double p99Latency = oldMetricsInJSON.getDoubleValue("mean99");
            double maxLatency = oldMetricsInJSON.getDoubleValue("maxLatency");

            JSONObject newMetricsInJSON = new JSONObject();
            newMetricsInJSON.put("totalBytes", totalBytes + bucket.totalBytes);
            newMetricsInJSON.put("count", count + bucket.count);
            newMetricsInJSON.put("noTimeCount", noTimeCount + bucket.noTimeCount);
            newMetricsInJSON.put("malformedCount", malformedCount + bucket.malformedCount);

            newMetricsInJSON.put("mean", (meanLatency * count + bucket.totalLatency) / (count + bucket.count));
            newMetricsInJSON.put("mean95", Math.max(p95Latency, bucket.latency95));
            newMetricsInJSON.put("mean99", Math.max(p99Latency, bucket.latency99));
            newMetricsInJSON.put("maxLatency", Math.max(maxLatency, bucket.maxLatency));

            logger.debug("Update existing bucket with beginTs={}, endTs={}, topicName={}, hostName={}, "
                + "tier={}, oldMetrics={}, newMetrics={}", beginDbTs, endDbTs, bucket.topicName, bucket.hostName,
                bucket.tier, oldMetricsInJSON.toString(), newMetricsInJSON.toString());

            updateStmt.setString(1, newMetricsInJSON.toString());
            updateStmt.setTimestamp(2, beginDbTs);
            updateStmt.setTimestamp(3, endDbTs);
            updateStmt.setString(4, bucket.topicName);
            updateStmt.setString(5, bucket.hostName);
            updateStmt.setString(6, bucket.datacenter);
            updateStmt.setString(7, bucket.tier);
            updateStmt.executeUpdate();
            UPDATED_RECORDS_COUNTER.mark();
          } else {
            insertStmt.setTimestamp(1, beginDbTs);
            insertStmt.setTimestamp(2, endDbTs);
            insertStmt.setString(3, bucket.topicName);
            insertStmt.setString(4, bucket.getMetricsAsJSON());
            insertStmt.setString(5, bucket.hostName);
            insertStmt.setString(6, bucket.datacenter);
            insertStmt.setString(7, bucket.tier);
            insertStmt.executeUpdate();
            INSERTED_RECORDS_COUNTER.mark();
          }

          if (beginTsInMs > latestTSSeenLastInsert) {
            latestTSSeenLastInsert = beginTsInMs;
          }
          if (beginTsInMs < earliestTSSeenLastInsert) {
            earliestTSSeenLastInsert = beginTsInMs;
          }
          REPORTED_COUNTER.mark();
        } finally {
          closeResultset(rs);
        }
      }
    }
  }

  private void closeDbResource(ResultSet rs, PreparedStatement stmt, Connection conn) {
    closeResultset(rs);
    closeStatement(stmt);
    closeConnection(conn);
  }

  private void closeConnection(Connection conn) {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      logger.warn("Got exception to close db connection", e);
    }
  }

  private void closeStatement(PreparedStatement stmt) {
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (SQLException e) {
      logger.warn("Got exception to close prepared statement", e);
    }
  }

  private void closeResultset(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      logger.warn("Got exception to close result set", e);
    }
  }

  private void rollback(Connection conn) {
    try {
      if (conn != null) {
        conn.rollback();
      }
    } catch (SQLException e) {
      logger.warn("Got exception to rollback transaction", e);
    }
  }

  @Override
  public String getType() {
    return AuditReporterFactory.DB_AUDIT_REPORTER;
  }

  public Connection getConnection() throws SQLException {
    return ds.getConnection();
  }
}
