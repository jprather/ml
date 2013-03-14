/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.science.ml.parallel.pobject;

import java.util.List;
import java.util.SortedMap;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.materialize.pobject.PObjectImpl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ListOfListsPObject<V> extends PObjectImpl<Pair<Pair<Integer, Integer>, V>, List<List<V>>> {
  private V emptyValue;
  private int[] expected;
  
  public ListOfListsPObject(PCollection<Pair<Pair<Integer, Integer>, V>> collect, int[] expected,
      V empty) {
    super(collect);
    this.emptyValue = empty;
    this.expected = expected;
  }

  @Override
  protected List<List<V>> process(Iterable<Pair<Pair<Integer, Integer>, V>> input) {
    SortedMap<Integer, SortedMap<Integer, V>> sm = Maps.newTreeMap();
    for (Pair<Pair<Integer, Integer>, V> p : input) {
      int center = p.first().first();
      SortedMap<Integer, V> inner = sm.get(center);
      if (inner == null) {
        inner = Maps.newTreeMap();
        sm.put(center, inner);
      }
      inner.put(p.first().second(), p.second());
    }
    List<List<V>> ret = Lists.newArrayList();
    for (int i = 0; i < expected.length; i++) {
      SortedMap<Integer, V> e = sm.get(i);
      if (e == null) {
        ret.add(null);
      } else {
        List<V> v = Lists.newArrayList();
        for (int j = 0; j < expected[i]; j++) {
          V value = e.get(j);
          if (value == null) {
            v.add(emptyValue);
          } else {
            v.add(value);
          }
        }
        ret.add(v);
      }
    }
    return ret;
  }
}