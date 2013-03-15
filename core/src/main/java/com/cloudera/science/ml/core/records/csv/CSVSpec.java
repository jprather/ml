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
package com.cloudera.science.ml.core.records.csv;

import java.util.List;
import java.util.Map;

import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Spec;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 */
public class CSVSpec implements Spec {

  private final DataType dataType;
  private final Map<String, CSVFieldSpec> fields;
  
  public CSVSpec(DataType dataType) {
    this.dataType = dataType;
    this.fields = ImmutableMap.of();
  }
  
  public CSVSpec(DataType dataType, String name, DataType type, Object... others) {
    this.dataType = dataType;
    this.fields = Maps.newLinkedHashMap();
    this.fields.put(name, new CSVFieldSpec(name, 0, type));
    for (int i = 0; i < others.length; i += 2) {
      String n = (String) others[i];
      DataType dt = (DataType) others[i + 1];
      fields.put(n, new CSVFieldSpec(n, i + 1, dt));
    }
  }
  
  @Override
  public int size() {
    return fields.size();
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public List<String> getFieldNames() {
    return Lists.newArrayList(fields.keySet());
  }

  @Override
  public FieldSpec getField(String fieldName) {
    return fields.get(fieldName);
  }

  @Override
  public Spec arrayValue() {
    throw new UnsupportedOperationException("CSVs do not support arrays");
  }

  @Override
  public Spec mapKey() {
    throw new UnsupportedOperationException("CSVs do not support maps");
  }

  @Override
  public Spec mapValue() {
    throw new UnsupportedOperationException("CSVs do not support maps");
  }
}
