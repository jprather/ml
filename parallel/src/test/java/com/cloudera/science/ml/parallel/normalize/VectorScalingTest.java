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

import static org.junit.Assert.assertEquals;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.cloudera.science.ml.core.Vectors;
import com.cloudera.science.ml.parallel.types.MLAvros;
import com.google.common.collect.ImmutableList;

public class VectorScalingTest {
  private PCollection<Vector> vecs = MemPipeline.typedCollectionOf(
      MLAvros.vector(),
      Vectors.of(1.0, 2.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(2.0, 1.0),
      Vectors.of(2.0, 2.0));
  
  @Test
  public void testScaling() throws Exception {
    PCollection<Vector> scaled = VectorScaling.scale(vecs, new double[] { 0.5, 0.25 });
    assertEquals(ImmutableList.of(Vectors.of(0.5, 0.5),
        Vectors.of(0.5, 0.25), Vectors.of(1.0, 0.25),
        Vectors.of(1.0, 0.5)), scaled.materialize());
  }
}
