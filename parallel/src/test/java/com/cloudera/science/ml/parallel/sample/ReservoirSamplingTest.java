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
package com.cloudera.science.ml.parallel.sample;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Random;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

import com.cloudera.science.ml.parallel.sample.ReservoirSampling;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 *
 */
public class ReservoirSamplingTest {

  private Random r = new Random(1729L);
  
  private PCollection<Pair<String, Double>> values = MemPipeline.typedCollectionOf(
      Writables.pairs(Writables.strings(), Writables.doubles()),
      ImmutableList.of(
        Pair.of("foo", 200.0),
        Pair.of("bar", 400.0),
        Pair.of("baz", 100.0),
        Pair.of("biz", 100.0)));
  
  @Test
  public void testWRS() throws Exception {
    Map<String, Integer> histogram = Maps.newHashMap();
    
    for (int i = 0; i < 100; i++) {
      PCollection<String> sample = ReservoirSampling.weightedSample(values, 2, r);
      for (String s : sample.materialize()) {
        if (!histogram.containsKey(s)) {
          histogram.put(s, 1);
        } else {
          histogram.put(s, 1 + histogram.get(s));
        }
      }
    }
    
    Map<String, Integer> expected = ImmutableMap.of(
        "foo", 48, "bar", 78, "baz", 38, "biz", 36);
    assertEquals(expected, histogram);
  }
}
