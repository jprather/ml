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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.google.common.collect.Lists;

/**
 *
 */
public class CSVRecord implements Record {

  private CSVSpec spec;
  private List<Object> values;
  
  public CSVRecord(CSVSpec spec, Object... values) {
    this(spec, Arrays.asList(values));
  }
  
  public CSVRecord(CSVSpec spec, List<Object> values) {
    this.spec = spec;
    this.values = values;
  }
  
  @Override
  public Spec getSpec() {
    return spec;
  }

  @Override
  public Record copy(boolean deep) {
    if (!deep) {
      List<Object> v = Arrays.asList(new Object[values.size()]);
      return new CSVRecord(spec, v);
    } else {
      List<Object> v = Lists.newArrayList(values);
      return new CSVRecord(spec, v);
    }
  }
  
  @Override
  public Object get(String fieldName) {
    return values.get(spec.getField(fieldName).position());
  }
  
  @Override
  public <T> T get(String fieldName, Class<T> typeClass) {
    return typeClass.cast(get(fieldName));
  }

  @Override
  public Boolean getBoolean(String fieldName) {
    return get(fieldName, Boolean.class);
  }

  @Override
  public Double getDouble(String fieldName) {
    return get(fieldName, Double.class);
  }

  @Override
  public Integer getInteger(String fieldName) {
    return get(fieldName, Integer.class);
  }

  @Override
  public List<?> getList(String fieldName) {
    throw new UnsupportedOperationException("CSVRecords do not support lists");
  }

  @Override
  public <T> List<T> getList(String fieldName, Class<T> typeClass) {
    throw new UnsupportedOperationException("CSVRecords do not support lists");
  }

  @Override
  public Long getLong(String fieldName) {
    return get(fieldName, Long.class);
  }

  @Override
  public Map<?, ?> getMap(String fieldName) {
    throw new UnsupportedOperationException("CSVRecords do not support maps");
  }

  @Override
  public <K, V> Map<K, V> getMap(String fieldName, Class<K> keyClass, Class<V> valueClass) {
    throw new UnsupportedOperationException("CSVRecords do not support maps");
  }

  @Override
  public String getString(String fieldName) {
    return get(fieldName, String.class);
  }

  @Override
  public Record getRecord(String fieldName) {
    throw new UnsupportedOperationException("CSVRecords do not support nested records");
  }

  @Override
  public Record set(String fieldName, Object value) {
    values.set(spec.getField(fieldName).position(), value);
    return this;
  }

  @Override
  public Record setBoolean(String fieldName, Boolean value) {
    return set(fieldName, value);
  }

  @Override
  public Record setDouble(String fieldName, Double value) {
    return set(fieldName, value);
  }

  @Override
  public Record setInteger(String fieldName, Integer value) {
    return set(fieldName, value);
  }

  @Override
  public Record setList(String fieldName, List<?> value) {
    throw new UnsupportedOperationException("CSVRecords do not support maps");
  }

  @Override
  public Record setLong(String fieldName, Long value) {
    return set(fieldName, value);
  }

  @Override
  public Record setMap(String fieldName, Map<?, ?> value) {
    throw new UnsupportedOperationException("CSVRecords do not support maps");
  }

  @Override
  public Record setString(String fieldName, String value) {
    return set(fieldName, value);
  }

  @Override
  public Record setRecord(String fieldName, Record value) {
    throw new UnsupportedOperationException("CSVRecords do not support records");
  }

}
