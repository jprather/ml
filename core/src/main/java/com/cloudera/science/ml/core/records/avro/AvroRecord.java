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
package com.cloudera.science.ml.core.records.avro;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;

/**
 *
 */
public class AvroRecord implements Record {

  private GenericData.Record impl;
  
  public AvroRecord(GenericData.Record impl) {
    this.impl = impl;
  }
  
  public Record copy(boolean deep) {
    if (deep) {
      return new AvroRecord(new GenericData.Record(impl, true));
    } else {
      return new AvroRecord(new GenericData.Record(impl.getSchema()));
    }
  }
  
  @Override
  public Spec getSpec() {
    return new AvroSpec(impl.getSchema());
  }

  @Override
  public Object get(String fieldName) {
    return impl.get(fieldName);
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
    return get(fieldName, List.class);
  }

  @Override
  public <T> List<T> getList(String fieldName, final Class<T> typeClass) {
    return (List<T>) get(fieldName, List.class);
  }

  @Override
  public Long getLong(String fieldName) {
    return get(fieldName, Long.class);
  }

  @Override
  public Map<?, ?> getMap(String fieldName) {
    return get(fieldName, Map.class);
  }

  @Override
  public <K, V> Map<K, V> getMap(String fieldName, Class<K> keyClass, Class<V> valueClass) {
    return (Map<K, V>) get(fieldName, Map.class);
  }

  @Override
  public String getString(String fieldName) {
    return get(fieldName, String.class);
  }

  @Override
  public Record getRecord(String fieldName) {
    GenericData.Record rec = get(fieldName, GenericData.Record.class);
    return new AvroRecord(rec);
  }

  @Override
  public Record set(String fieldName, Object value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setBoolean(String fieldName, Boolean value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setDouble(String fieldName, Double value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setInteger(String fieldName, Integer value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setList(String fieldName, List<?> value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setLong(String fieldName, Long value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setMap(String fieldName, Map<?, ?> value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setString(String fieldName, String value) {
    impl.put(fieldName, value);
    return this;
  }

  @Override
  public Record setRecord(String fieldName, Record value) {
    impl.put(fieldName, value);
    return this;
  }

}
