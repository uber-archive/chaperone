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

import org.codehaus.jackson.JsonToken
import org.codehaus.jackson.map.ObjectMapper

trait ITimestampExtractor {
  def getTimestamp(msg: Array[Byte]): Double
}

object JsonTopTsExtractor extends ITimestampExtractor {
  private val TIMESTAMP_FIELD_SHORTNAME = "ts"
  private val QUOTED_TIMESTAMP_FIELD_SHORTNAME = "\"ts\""
  private val TIMESTAMP_FIELD_FULLNAME = "timestamp"
  private val QUOTED_TIMESTAMP_FIELD_FULLNAME = "\"timestamp\""
  private val mapper = new ObjectMapper

  override def getTimestamp(msg: Array[Byte]): Double = {
    scanForTimestamp(msg)
  }

  private def scanForTimestamp(msg: Array[Byte]): Double = {
    if (msg == null || msg.length == 0) {
      return -1
    }

    val parser = mapper.getJsonFactory.createJsonParser(msg)
    try {
      var leftBracketCnt = 0
      var curToken = parser.nextToken()
      while (curToken != null) {
        if (leftBracketCnt == 1 && curToken == JsonToken.FIELD_NAME) {
          val fieldName = parser.getCurrentName
          if (fieldName.equalsIgnoreCase(TIMESTAMP_FIELD_SHORTNAME) ||
            fieldName.equalsIgnoreCase(QUOTED_TIMESTAMP_FIELD_SHORTNAME) ||
            fieldName.equalsIgnoreCase(TIMESTAMP_FIELD_FULLNAME) ||
            fieldName.equalsIgnoreCase(QUOTED_TIMESTAMP_FIELD_FULLNAME)) {
            parser.nextToken()
            val ts = parser.getDoubleValue
            parser.close()
            return ts
          }
        } else if (curToken == JsonToken.START_OBJECT || curToken == JsonToken.START_ARRAY) {
          leftBracketCnt += 1
        } else if (curToken == JsonToken.END_OBJECT || curToken == JsonToken.END_ARRAY) {
          leftBracketCnt -= 1
        }
        curToken = parser.nextToken()
      }
    } finally {
      parser.close()
    }
    -1
  }
}
