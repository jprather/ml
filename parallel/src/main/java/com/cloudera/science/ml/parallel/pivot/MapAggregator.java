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
package com.cloudera.science.ml.parallel.pivot;

import java.util.Map;

import org.apache.crunch.fn.Aggregators.SimpleAggregator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 *
 */
public class MapAggregator extends SimpleAggregator<Map<String, Stat>> {

  private Map<String, Stat> values = Maps.newHashMap();
  
  @Override
  public void reset() {
    values.clear();
  }

  @Override
  public Iterable<Map<String, Stat>> results() {
    return ImmutableList.of(values);
  }

  @Override
  public void update(Map<String, Stat> other) {
    for (Map.Entry<String, Stat> e : other.entrySet()) {
      if (values.containsKey(e.getKey())) {
        values.get(e.getKey()).merge(e.getValue());
      } else {
        values.put(e.getKey(), e.getValue());
      }
    }
  }

}
