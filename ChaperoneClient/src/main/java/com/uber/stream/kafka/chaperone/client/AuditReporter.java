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

import java.io.IOException;

/**
 * Interface to report auditing message to configured output channels like Kafka,
 * Graphite, etc. Just need one AuditReporter per library.
 */
public interface AuditReporter {

  /**
   * Builds an audit message to encapsulate all the audit information.
   * 
   * @param topicName The name of the topic to audit
   * @param timeBucket The time bucket metadata information to collect
   * @return
   */
  AuditMessage buildAuditMessage(String topicName, TimeBucketMetadata timeBucket);

  /**
   * Report an audit message. This method is implemented differently depending
   * on what reporting mechanism is in use.
   * 
   * @param message The audit message to report
   * @throws IOException
   */
  void reportAuditMessage(AuditMessage message) throws IOException;

  /**
   * Blocking call to shutdown the reporter service. Any pending audit
   * messages should be sent out before the call returns.
   */
  void shutdown();
}
