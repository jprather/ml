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

/**
 *
 */
public interface Record {
  Spec getSpec();
 
  Record copy(boolean deep);
  
  Boolean getBoolean(int index);

  Boolean getBoolean(String fieldName);

  Double getDouble(int index);
  
  Double getDouble(String fieldName);
  
  Integer getInteger(int index);

  Integer getInteger(String fieldName);

  Long getLong(int index);

  Long getLong(String fieldName);

  String getString(int index);
  
  String getString(String fieldName);
  
  Record set(int index, Object value);
  
  Record set(String fieldName, Object value);
  
  String getAsString(int index);
  
  double getAsDouble(int index);
}
