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

import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.mirrormaker.controller.validation.SourceKafkaClusterValidationManager;
import com.uber.stream.kafka.mirrormaker.controller.validation.ValidationManager;

/**
 * Validate idealState and externalView also update related metrics.
 */
public class ValidationRestletResource extends ServerResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ValidationRestletResource.class);
  private final ValidationManager _validationManager;
  private final SourceKafkaClusterValidationManager _srcKafkaValidationManager;

  public ValidationRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);

    _validationManager = (ValidationManager) getApplication().getContext()
        .getAttributes().get(ValidationManager.class.toString());
    if (getApplication().getContext().getAttributes()
        .containsKey(SourceKafkaClusterValidationManager.class.toString())) {
      _srcKafkaValidationManager =
          (SourceKafkaClusterValidationManager) getApplication().getContext()
              .getAttributes().get(SourceKafkaClusterValidationManager.class.toString());
    } else {
      _srcKafkaValidationManager = null;
    }
  }

  @Override
  @Get
  public Representation get() {
    final String option = (String) getRequest().getAttributes().get("option");
    if ("srcKafka".equals(option)) {
      if (_srcKafkaValidationManager == null) {
        LOGGER.warn("SourceKafkaClusterValidationManager is null!");
        return new StringRepresentation("SrcKafkaValidationManager is not been initialized!");
      }
      LOGGER.info("Trying to call validation on source kafka cluster!");
      return new StringRepresentation(_srcKafkaValidationManager.validateSourceKafkaCluster());
    } else {
      LOGGER.info("Trying to call validation on current cluster!");
      return new StringRepresentation(_validationManager.validateExternalView());
    }
  }

}
