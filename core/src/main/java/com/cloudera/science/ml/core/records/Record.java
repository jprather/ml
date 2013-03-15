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
package com.cloudera.science.ml.core.records;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface Record {
  Spec getSpec();
 
  Record copy(boolean deep);
  
  Object get(String fieldName);
  
  <T> T get(String fieldName, Class<T> typeClass);
  
  Boolean getBoolean(String fieldName);
  
  Double getDouble(String fieldName);
  
  Integer getInteger(String fieldName);
  
  List<?> getList(String fieldName);
  
  <T> List<T> getList(String fieldName, Class<T> typeClass);
  
  Long getLong(String fieldName);
  
  Map<?, ?> getMap(String fieldName);
  
  <K, V> Map<K, V> getMap(String fieldName, Class<K> keyClass, Class<V> valueClass);
  
  String getString(String fieldName);
  
  Record getRecord(String fieldName);
  
  Record set(String fieldName, Object value);
  
  Record setBoolean(String fieldName, Boolean value);
  
  Record setDouble(String fieldName, Double value);
  
  Record setInteger(String fieldName, Integer value);
  
  Record setList(String fieldName, List<?> value);
  
  Record setLong(String fieldName, Long value);
  
  Record setMap(String fieldName, Map<?, ?> value);

  Record setString(String fieldName, String value);
  
  Record setRecord(String fieldName, Record value);
}
