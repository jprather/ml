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
package com.cloudera.science.ml.core.records.vectors;

import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.records.BasicSpec;
import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;

/**
 *
 */
public class VectorRecord implements Record {

  private final Vector vector;
  
  public VectorRecord(Vector vector) {
    this.vector = vector;
  }
  
  @Override
  public Spec getSpec() {
    return new BasicSpec(DataType.DOUBLE, vector.size());
  }

  @Override
  public Record copy(boolean deep) {
    if (deep) {
      return new VectorRecord(vector.like().assign(vector));
    } else {
      return new VectorRecord(vector.like());
    }
  }

  public Vector getVector() {
    return vector;
  }
  
  @Override
  public Boolean getBoolean(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Boolean getBoolean(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Double getDouble(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Double getDouble(int index) {
    return vector.getQuick(index);
  }

  @Override
  public Integer getInteger(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer getInteger(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getLong(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getLong(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Record set(int index, Object value) {
    vector.setQuick(index, ((Number) value).doubleValue());
    return this;
  }

  @Override
  public Record set(String fieldName, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getAsString(int index) {
    return String.valueOf(vector.getQuick(index));
  }

  @Override
  public double getAsDouble(int index) {
    return vector.getQuick(index);
  }
}
