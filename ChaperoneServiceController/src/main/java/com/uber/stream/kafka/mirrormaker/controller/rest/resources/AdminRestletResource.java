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

package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;

/**
 * AdminRestletResource is used to control auto balancing enable/disalbe.
 *  
 * @author xiangfu
 *
 */
public class AdminRestletResource extends ServerResource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AdminRestletResource.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public AdminRestletResource() {
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    final String opt = (String) getRequest().getAttributes().get("opt");
    if ("disable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoBalancing();
      LOGGER.info("Disabled autobalancing!");
      return new StringRepresentation("Disabled autobalancing!\n");
    } else if ("enable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoBalancing();
      LOGGER.info("Enabled autobalancing!");
      return new StringRepresentation("Enabled autobalancing!\n");
    }
    LOGGER.info("No valid input!");
    return new StringRepresentation("No valid input!\n");
  }

}
