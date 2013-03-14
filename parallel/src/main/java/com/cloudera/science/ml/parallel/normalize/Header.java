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
package com.cloudera.science.ml.parallel.normalize;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 *
 */
public class Header implements Serializable {

  private static Function<String, Integer> AS_INT = new Function<String, Integer>() {
    @Override
    public Integer apply(String input) {
      return Integer.valueOf(input);
    }
  };
  
  private final List<String> fieldNames;
 
  public Header() {
    this(ImmutableList.<String>of());
  }
  
  public Header(List<String> columnFields) {
    this.fieldNames = columnFields;
  }
  
  public Header(String csv, String delim) {
    this.fieldNames = Arrays.asList(csv.split(delim));
  }
  
  public int getNumFields() {
    return fieldNames.size();
  }
  
  public String getFieldName(int index) {
    if (index >= fieldNames.size()) {
      return null;
    }
    return fieldNames.get(index);
  }
  
  public int getFieldId(String nameOrIndex) {
    if (fieldNames.contains(nameOrIndex)) {
      return getFromName().apply(nameOrIndex);
    } else {
      return AS_INT.apply(nameOrIndex);
    }
  }
  
  public List<Integer> getFieldIds(List<String> namesOrIndices) {
    if (namesOrIndices.isEmpty()) {
      return ImmutableList.<Integer>of();
    }
    String idx = namesOrIndices.get(0);
    Function<String, Integer> t = fieldNames.contains(idx) ? getFromName() : AS_INT;
    return Lists.transform(namesOrIndices, t);
  }
  
  private Function<String, Integer> getFromName() {
    return new Function<String, Integer>() {
      @Override
      public Integer apply(String input) {
        return fieldNames.indexOf(input);
      };
    };
  }
}
