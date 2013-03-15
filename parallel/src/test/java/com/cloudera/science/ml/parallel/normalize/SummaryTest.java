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
package com.cloudera.science.ml.parallel.normalize;

import static org.junit.Assert.assertEquals;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.cloudera.science.ml.core.vectors.Vectors;
import com.cloudera.science.ml.parallel.types.MLAvros;
import com.google.common.collect.ImmutableList;

public class SummaryTest {
  private PCollection<Vector> vecs = MemPipeline.typedCollectionOf(
      MLAvros.vector(),
      Vectors.of(1.0, 3.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(3.0, 1.0),
      Vectors.of(3.0, 3.0));
  
  @Test
  public void testZScores() {
    PCollection<Elements> elems = ElementsVector.convert(vecs);
    Summarizer sr = new Summarizer();
    Summary s = sr.build(elems).getValue();
    Standardizer stand = Standardizer.builder()
        .summary(s)
        .defaultTransform(Transform.Z)
        .build();
    assertEquals(ImmutableList.of(Vectors.of(-1, 1),
        Vectors.of(-1, -1), Vectors.of(1, -1),
        Vectors.of(1, 1)), stand.apply(elems, MLAvros.vector()).materialize());
  }
}
