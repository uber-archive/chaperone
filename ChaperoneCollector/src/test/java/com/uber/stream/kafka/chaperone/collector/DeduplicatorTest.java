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

import org.easymock.EasyMock;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * mock jedis
 */
public class DeduplicatorTest {
  @Test
  public void testNewMsg() throws InterruptedException {
    String auditHost = "dummyHost";
    String topicName = "dummyTopic";
    String msgUUID0 = "uuid0";
    String msgUUID1 = "uuid1";

    Deduplicator dedup =
        new Deduplicator(0, AuditConfig.INGESTER_REDIS_HOST, AuditConfig.INGESTER_REDIS_PORT,
            AuditConfig.INGESTER_REDIS_KEY_TTL_SEC, "", auditHost);

    Jedis jedis = EasyMock.createNiceMock(Jedis.class);
    EasyMock.expect(jedis.get(msgUUID0)).andReturn(null);
    EasyMock.expect(jedis.setex(msgUUID0, AuditConfig.INGESTER_REDIS_KEY_TTL_SEC, "0_101")).andReturn(null);
    EasyMock.expect(jedis.get(msgUUID1)).andReturn(null);
    EasyMock.expect(jedis.setex(msgUUID1, AuditConfig.INGESTER_REDIS_KEY_TTL_SEC, "1_111")).andReturn(null);
    EasyMock.replay(jedis);

    dedup.open(jedis);

    // NEW msg. new uuid
    boolean duplicated = dedup.isDuplicated(topicName, 0, 101, auditHost, msgUUID0);
    assertFalse(duplicated);

    // NEW msg. new uuid
    duplicated = dedup.isDuplicated(topicName, 1, 111, auditHost, msgUUID1);
    assertFalse(duplicated);

    dedup.close();

    EasyMock.verify(jedis);
  }

  @Test
  public void testOldDupMsg() throws InterruptedException {
    String auditHost = "dummyHost";
    String topicName = "dummyTopic";
    String msgUUID = "uuid0";
    int partitionId = 0;
    long offset = 101;
    String valueForKey = partitionId + "_" + offset;

    Deduplicator dedup =
        new Deduplicator(0, AuditConfig.INGESTER_REDIS_HOST, AuditConfig.INGESTER_REDIS_PORT,
            AuditConfig.INGESTER_REDIS_KEY_TTL_SEC, "", auditHost);

    Jedis jedis = EasyMock.createNiceMock(Jedis.class);
    EasyMock.expect(jedis.get(msgUUID)).andReturn(null);
    EasyMock.expect(jedis.setex(msgUUID, AuditConfig.INGESTER_REDIS_KEY_TTL_SEC, valueForKey)).andReturn(null);
    EasyMock.expect(jedis.get(msgUUID)).andReturn(valueForKey);
    EasyMock.expect(jedis.get(msgUUID)).andReturn(valueForKey);
    EasyMock.replay(jedis);

    dedup.open(jedis);

    // NEW msg. new uuid
    boolean duplicated = dedup.isDuplicated(topicName, partitionId, offset, auditHost, msgUUID);
    assertFalse(duplicated);

    // OLD msg. old uuid, old partition_offset
    duplicated = dedup.isDuplicated(topicName, partitionId, offset, auditHost, msgUUID);
    assertFalse(duplicated);

    // DUP msg. old uuid, new partition_offset
    duplicated = dedup.isDuplicated(topicName, partitionId, offset + 1, auditHost, msgUUID);
    assertTrue(duplicated);

    dedup.close();

    EasyMock.verify(jedis);
  }
}
