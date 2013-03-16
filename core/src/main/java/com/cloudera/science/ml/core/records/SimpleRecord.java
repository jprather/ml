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
public class SimpleRecord implements Record {

  private final Spec spec;
  private final Object[] values;
  
  public SimpleRecord(Spec spec) {
    this(spec, new Object[spec.size()]);
  }
  
  private SimpleRecord(Spec spec, Object[] values) {
    this.spec = spec;
    this.values = values;
  }
  
  @Override
  public Spec getSpec() {
    return spec;
  }

  @Override
  public Record copy(boolean deep) {
    if (deep) {
      Object[] v = new Object[spec.size()];
      System.arraycopy(values, 0, v, 0, values.length);
      return new SimpleRecord(spec, v);
    } else {
      return new SimpleRecord(spec);
    }
  }

  @Override
  public Object get(int index) {
    return values[index];
  }

  @Override
  public Boolean getBoolean(int index) {
    return (Boolean) values[index];
  }

  @Override
  public Boolean getBoolean(String fieldName) {
    return getBoolean(spec.getField(fieldName).position());
  }

  @Override
  public Double getDouble(int index) {
    return (Double) values[index];
  }

  @Override
  public Double getDouble(String fieldName) {
    return getDouble(spec.getField(fieldName).position());
  }

  @Override
  public Integer getInteger(int index) {
    return (Integer) values[index];
  }

  @Override
  public Integer getInteger(String fieldName) {
    return getInteger(spec.getField(fieldName).position());
  }

  @Override
  public Long getLong(int index) {
    return (Long) values[index];
  }

  @Override
  public Long getLong(String fieldName) {
    return getLong(spec.getField(fieldName).position());
  }

  @Override
  public String getString(int index) {
    return (String) values[index];
  }

  @Override
  public String getString(String fieldName) {
    return getString(spec.getField(fieldName).position());
  }

  @Override
  public Record set(int index, Object value) {
    values[index] = value;
    return this;
  }

  @Override
  public Record set(String fieldName, Object value) {
    values[spec.getField(fieldName).position()] = value;
    return this;
  }

  @Override
  public String getAsString(int index) {
    return values[index].toString();
  }

  @Override
  public double getAsDouble(int index) {
    return ((Number) values[index]).doubleValue();
  }
}
