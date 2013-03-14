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

import java.util.Collections;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.materialize.pobject.PObjectImpl;

import com.google.common.collect.Lists;

public class ListPObject<V> extends PObjectImpl<Pair<Integer, V>, List<V>> {

  public ListPObject(PCollection<Pair<Integer, V>> collect) {
    super(collect);
  }

  @Override
  protected List<V> process(Iterable<Pair<Integer, V>> iterable) {
    List<Pair<Integer, V>> list = Lists.newArrayList(iterable);
    Collections.sort(list);
    List<V> ret = Lists.newArrayList();
    for (int i = 0; i < list.size(); i++) {
      ret.add(list.get(i).second());
    }
    return ret;
  }
}