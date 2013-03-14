/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.ml.client.params;

import java.io.File;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.type.TypeReference;

public class JacksonParameters {

  private final ObjectMapper mapper = new ObjectMapper();
  
  public JacksonParameters() {
    SerializationConfig config = mapper.getSerializationConfig();
    config.setSerializationInclusion(Inclusion.NON_NULL);
    mapper.setVisibilityChecker(config.getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
  }
  
  public void save(Object object, File target) {
    try {
      ObjectWriter writer = mapper.defaultPrettyPrintingWriter();
      writer.writeValue(target, object);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public Map<String, Object> read(File jsonFile) {
    try {
      return (Map<String, Object>) mapper.readValue(jsonFile, Map.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public <T> T read(File jsonFile, TypeReference<T> ref) {
    try {
      return mapper.readValue(jsonFile, ref);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public <T> T read(File jsonFile, Class<T> clazz) {
    try {
      return mapper.readValue(jsonFile, clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
