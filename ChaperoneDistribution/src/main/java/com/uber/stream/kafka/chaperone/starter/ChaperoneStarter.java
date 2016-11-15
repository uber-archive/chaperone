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
package com.uber.stream.kafka.chaperone.starter;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.chaperone.collector.KafkaIngester;

import com.uber.stream.kafka.mirrormaker.controller.ControllerStarter;

import kafka.mirrormaker.MirrorMakerWorker;

/**
 * This is the entry point to start ChaperoneService controller and worker, as well as ChaperoneCollector
 * The 1st parameter indicates the module to start:
 * - startChaperoneServiceController means to start a controller
 * - startChaperoneServiceWorker means to start a worker
 * - startChaperoneCollector means to start an collector
 * The following parameters are for each module separately.
 */
public class ChaperoneStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChaperoneStarter.class);

  public static void main(String[] args) throws Exception {
    if (args.length > 1) {
      if (args[0].equalsIgnoreCase("startChaperoneServiceController")) {
        LOGGER.info("Trying to start ChaperoneService Controller with args: {}", Arrays.toString(args));
        ControllerStarter.main(args);
      } else if (args[0].equalsIgnoreCase("startChaperoneServiceWorker")) {
        LOGGER.info("Trying to start ChaperoneService Worker with args: {}", Arrays.toString(args));
        MirrorMakerWorker.main(args);
      } else if (args[0].equalsIgnoreCase("startChaperoneCollector")) {
        LOGGER.info("Trying to start ChaperoneCollector with args: {}", Arrays.toString(args));
        KafkaIngester.main(args);
      } else {
        LOGGER.error("Start script should provide the module(startChaperoneSeriviceController/"
            + "startChaperoneSeriviceWorker/startChaperoneCollector)"
            + " to start as the first parameter! Current args: {}", Arrays.toString(args));
      }
    } else {
      LOGGER.error(
          "Start script doesn't provide enough parameters! Current args: {}.", Arrays.toString(args));
    }
  }

}
