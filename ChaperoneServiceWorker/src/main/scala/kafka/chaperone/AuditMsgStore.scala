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
package kafka.chaperone

import java.sql._
import java.util
import java.util.concurrent._

import com.alibaba.fastjson.JSON
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.commons.dbcp2.BasicDataSource
import org.hsqldb.server.Server

/**
 * Keep AuditMsg locally as WAL for sending so that we have chance to resend them.
 * Once AuditMsg is sent out, it is marked as 'sentout' at best effort. Resending
 * multiple times does not matter because AuditMsg is idempotent.
 */
class AuditMsgStore(val currentEpochInMs: Long,
                    val jdbcDriver: String,
                    val dbUrl: String,
                    val dbUser: String,
                    val dbPass: String,
                    val dbServerPort: Int,
                    val tableName: String) extends Logging with KafkaMetricsGroup {
  private val CREATE_TABLE = "CREATE CACHED TABLE IF NOT EXISTS %s " +
    "(uuid        VARCHAR(128), " +
    " time_begin  TIMESTAMP, " +
    " time_end    TIMESTAMP, " +
    " topic_name  VARCHAR(128), " +
    " metrics     VARCHAR(1024), " +
    " host_name   VARCHAR(64), " +
    " audit_dc    VARCHAR(64), " +
    " tier        VARCHAR(64), " +
    " sent        BOOLEAN, " +
    " PRIMARY KEY (uuid))"

  // as default 2PL is used, the lock granularity is table. Underlying, MVCC uses row lock.
  // MVCC is good for us, because we follow single-writer principle thus no contention on same row
  private val SET_TO_MVCC = "SET DATABASE TRANSACTION CONTROL MVCC"

  private val CREATE_INDEX_ON_TIME_BEGIN = "CREATE INDEX %s_on_time_begin ON %s (time_begin, time_end) "
  private val CREATE_INDEX_ON_SEND = "CREATE INDEX %s_on_sent ON %s (sent) "

  private val INSERT_SQL = "INSERT INTO %s (uuid, time_begin, time_end, topic_name, metrics, host_name, audit_dc, tier, sent) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

  private val DELETE_SQL = "DELETE FROM %s WHERE time_begin < ? AND sent=true"

  private val SELECT_UNSENT_SQL = "SELECT uuid, time_begin, time_end, topic_name, " +
    " metrics, host_name, audit_dc, tier " +
    " FROM %s WHERE time_begin < ? AND sent=false LIMIT ?"

  private val MARK_SENT_SQL = "UPDATE %s SET sent=true WHERE uuid=?"

  private val CHECK_SENT_SQL = "SELECT sent FROM %s WHERE uuid=?"

  private val failToRemoveRecord = newMeter("failToRemoveRecordPerSec", "dbstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val failToMarkRecord = newMeter("failToMarkRecordPerSec", "dbstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val failToAddRecord = newMeter("failToAddRecordPerSec", "dbstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))
  private val failToGetUnsentRecord = newMeter("failToGetUnsentRecordPerSec", "dbstore", TimeUnit.SECONDS, Map("metricsType" -> "meter"))

  // dbServer is for command line tool to read the db files
  private var dbServer: Server = null

  private val ds = new BasicDataSource
  ds.setUrl(dbUrl)
  ds.setDriverClassName(jdbcDriver)
  ds.setUsername(dbUser)
  ds.setPassword(dbPass)

  def getDbConnection(autoCommit: Boolean): Connection = {
    val dbConnection = ds.getConnection
    dbConnection.setAutoCommit(autoCommit)
    dbConnection
  }

  private def maybeCreateTable() {
    var conn: Connection = null
    try {
      conn = getDbConnection(false)

      var sql = SET_TO_MVCC
      info("Set database to MVCC for high level concurrency")
      var stmt = conn.prepareStatement(sql)
      stmt.executeUpdate()
      closeStatement(stmt)

      sql = CREATE_TABLE.format(tableName)
      info("Create table with sql=%s".format(sql))
      stmt = conn.prepareStatement(sql)
      stmt.executeUpdate()
      closeStatement(stmt)

      // yuk, there is no create index if not exists syntax sugar.
      try {
        sql = CREATE_INDEX_ON_TIME_BEGIN.format(tableName, tableName)
        info("Create table index with sql=%s".format(sql))
        stmt = conn.prepareStatement(sql)
        stmt.executeUpdate()
      }
      catch {
        case e: SQLSyntaxErrorException =>
          if (!e.getMessage.contains("object name already exists")) {
            throw e
          }
      }
      finally {
        closeStatement(stmt)
      }

      try {
        sql = CREATE_INDEX_ON_SEND.format(tableName, tableName)
        info("Create table index with sql=%s".format(sql))
        stmt = conn.prepareStatement(sql)
        stmt.executeUpdate()
      }
      catch {
        case e: SQLSyntaxErrorException =>
          if (!e.getMessage.contains("object name already exists")) {
            throw e
          }
      }
      finally {
        closeStatement(stmt)
      }

      conn.commit()
    }
    catch {
      case e: Exception =>
        warn("Got exception to create table and index", e)
        rollback(conn)
    }
    finally {
      closeConnection(conn)
    }
  }

  private def startDbServer() {
    if (dbServerPort == -1) {
      warn("DbServer is disabled")
      return
    }

    dbServer = new Server()
    dbServer.setPort(dbServerPort)

    // dbUrl is like jdbc:hsqldb:file:/tmp/chaperone2/test
    info("Start db server for dbname=%s, dbpath=%s".format(dbUrl.substring(dbUrl.lastIndexOf('/') + 1),
      dbUrl.substring(dbUrl.lastIndexOf(':') + 1)))

    dbServer.setDatabaseName(0, dbUrl.substring(dbUrl.lastIndexOf('/') + 1))
    dbServer.setDatabasePath(0, dbUrl.substring(dbUrl.lastIndexOf(':') + 1))
    dbServer.start()
  }

  def removeOldRecord(watermarkMs: Long): Int = {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    var affected: Int = 0
    try {
      conn = getDbConnection(false)
      stmt = conn.prepareStatement(DELETE_SQL.format(tableName))
      val dbTs = new Timestamp(watermarkMs)
      stmt.setTimestamp(1, dbTs)
      affected = stmt.executeUpdate
      conn.commit()
      info("Removed count=%d old records per timestamp=%s".format(affected, watermarkMs))
      affected
    }
    catch {
      case e: Exception =>
        failToRemoveRecord.mark()
        warn("Got exception to remove old records", e)
        rollback(conn)
        0
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t
    }
    finally {
      closeDbResource(null, stmt, conn)
    }
  }

  def open() {
    info("Open AuditMsgStore...")
    maybeCreateTable()
    startDbServer()
  }

  def close() {
    info("Close AuditMsgStore...")

    if (dbServer != null) {
      info("Stop db server...")
      dbServer.shutdown()
    }

    info("Close datasource")
    ds.close()
  }

  private def appendAuditMsgs(topicName: String,
                              insertStmt: PreparedStatement,
                              auditMsgs: util.LinkedList[LocalAuditMsg]) {
    val itr = auditMsgs.iterator()
    while (itr.hasNext) {
      val auditMsg = itr.next

      debug("Insert/Update auditMsg=" + auditMsg.toString)

      if (auditMsg.count > 0) {
        val beginTsInMs: Long = TimeUnit.SECONDS.toMillis(auditMsg.timeBeginInSec.toLong)
        val endTsInMs: Long = TimeUnit.SECONDS.toMillis(auditMsg.timeEndInSec.toLong)
        val beginDbTs: Timestamp = new Timestamp(beginTsInMs)
        val endDbTs: Timestamp = new Timestamp(endTsInMs)

        debug(("Insert auditMsg with uuid=%s, beginTs=%s, endTs=%s, topicName=%s, " +
          "hostName=%s, datacenter=%s, tier=%s, newMetrics=%s").format(
          auditMsg.uuid, beginDbTs.toString, endDbTs.toString, topicName,
          auditMsg.hostName, auditMsg.datacenter, auditMsg.tier, auditMsg.getMetricsAsJSON))

        insertStmt.setString(1, auditMsg.uuid)
        insertStmt.setTimestamp(2, beginDbTs)
        insertStmt.setTimestamp(3, endDbTs)
        insertStmt.setString(4, topicName)
        insertStmt.setString(5, auditMsg.getMetricsAsJSON)
        insertStmt.setString(6, auditMsg.hostName)
        insertStmt.setString(7, auditMsg.datacenter)
        insertStmt.setString(8, auditMsg.tier)
        insertStmt.setBoolean(9, false)
        insertStmt.executeUpdate
      }
    }
  }

  def store(topicName: String, auditMsgs: util.LinkedList[LocalAuditMsg]) {
    debug("Storing AuditMsg to local store for topic=%s".format(topicName))

    // retry until done successfully, backpressure is imposed to kafka consumers via Disruptor.
    var retryTimes = 1
    while (true) {
      var conn: Connection = null
      var insertStmt: PreparedStatement = null

      try {
        conn = getDbConnection(false)
        insertStmt = conn.prepareStatement(INSERT_SQL.format(tableName))
        appendAuditMsgs(topicName, insertStmt, auditMsgs)
        conn.commit()
        return
      } catch {
        case e: Exception =>
          failToAddRecord.mark()
          val sleepInMs = Math.max(500, Math.min(60000, retryTimes * 500))
          retryTimes += 1
          warn("Got exception to insert new auditMsgs=%s, retryTimes=%d, sleepInMs=%d".format(
            auditMsgs, retryTimes, sleepInMs), e)
          rollback(conn)
          Thread.sleep(sleepInMs)
      }
      finally {
        closeDbResource(null, insertStmt, conn)
      }
    }
  }

  def getUnsentAuditMsg(buffer: util.LinkedList[LocalAuditMsg], limit: Int) {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = getDbConnection(true)
      stmt = conn.prepareStatement(SELECT_UNSENT_SQL.format(tableName))
      val dbTs = new Timestamp(currentEpochInMs)
      stmt.setTimestamp(1, dbTs)
      stmt.setInt(2, limit)
      rs = stmt.executeQuery()
      while (rs.next()) {
        val timeBegin: Double = rs.getTimestamp("time_begin").getTime / 1000.0
        val timeEnd: Double = rs.getTimestamp("time_end").getTime / 1000.0
        val json = JSON.parseObject(rs.getString("metrics"))

        val record = new LocalAuditMsg(rs.getString("uuid"), timeBegin, timeEnd,
          rs.getString("topic_name"), rs.getString("host_name"),
          rs.getString("audit_dc"), rs.getString("tier"),
          json.getLongValue("totalBytes"),
          json.getLongValue("count"), json.getLongValue("noTimeCount"), json.getLongValue("malformedCount"),
          json.getDoubleValue("meanLatency"), json.getDoubleValue("latency95"), json.getDoubleValue("latency99"),
          json.getDoubleValue("maxLatency"))

        debug("Getting unsent AuditMsg=%s from AuditMsgStore".format(record))
        buffer.add(record)
      }
      info("Got count=%d old records per timestamp=%s".format(buffer.size(), currentEpochInMs))
    }
    catch {
      case e: Exception =>
        failToGetUnsentRecord.mark()
        warn("Got exception to get unsent AuditMsg", e)
      case t: Throwable =>
        // method called in executor thread should have a Throwable case
        // to log the fatal error, which otherwise is hidden.
        fatal("Got fatal error", t)
        throw t
    }
    finally {
      closeDbResource(rs, stmt, conn)
    }
  }

  def checkSentAuditMsg(uuid: String): Boolean = {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      debug("Check AuditMsg if sent for uuid=" + uuid)
      conn = getDbConnection(true)
      stmt = conn.prepareStatement(CHECK_SENT_SQL.format(tableName))
      stmt.setString(1, uuid)
      rs = stmt.executeQuery()
      if (rs.next()) {
        return rs.getBoolean("sent")
      }
    }
    catch {
      case e: Exception =>
        warn("Got exception to check AuditMsg if sent", e)
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t
    }
    finally {
      closeDbResource(rs, stmt, conn)
    }
    false
  }

  def markAuditMsg(uuid: String) {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    try {
      conn = getDbConnection(false)
      stmt = conn.prepareStatement(MARK_SENT_SQL.format(tableName))
      stmt.setString(1, uuid)
      stmt.executeUpdate()
      conn.commit()
      debug("Mark AuditMsg as sent for uuid=" + uuid)
    }
    catch {
      case e: Exception =>
        failToMarkRecord.mark()
        warn("Got exception to mark AuditMsg sent", e)
        rollback(conn)
      case t: Throwable =>
        fatal("Got fatal error", t)
        throw t
    }
    finally {
      closeDbResource(null, stmt, conn)
    }
  }

  def closeDbResource(rs: ResultSet, stmt: Statement, conn: Connection) {
    closeResultset(rs)
    closeStatement(stmt)
    closeConnection(conn)
  }

  def closeConnection(conn: Connection) {
    try {
      if (conn != null) {
        conn.close()
      }
    }
    catch {
      case e: SQLException =>
        warn("Got exception to close db connection", e)
    }
  }

  def closeStatement(stmt: Statement) {
    try {
      if (stmt != null) {
        stmt.close()
      }
    }
    catch {
      case e: SQLException =>
        warn("Got exception to close prepared statement", e)
    }
  }

  def closeResultset(rs: ResultSet) {
    try {
      if (rs != null) {
        rs.close()
      }
    }
    catch {
      case e: SQLException =>
        warn("Got exception to close result set", e)
    }
  }

  private def rollback(conn: Connection) {
    try {
      if (conn != null) {
        conn.rollback()
      }
    }
    catch {
      case e: SQLException =>
        warn("Got exception to rollback transaction", e)
    }
  }
}
