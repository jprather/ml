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
package com.cloudera.science.ml.kmeans.parallel;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Random;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.cloudera.science.ml.kmeans.core.KMeans;
import com.cloudera.science.ml.parallel.crossfold.Crossfold;
import com.cloudera.science.ml.parallel.types.MLAvros;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class KMeansParallelTest {
  
  private PCollection<Vector> vecs = MemPipeline.typedCollectionOf(
      MLAvros.vector(),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0),
      Vectors.of(2.0, 1.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(5.0, 4.0),
      Vectors.of(4.0, 3.0));
  
  private KMeansParallel kmp;
  private Random r = new Random(29L);
  
  @Before
  public void setUp() throws Exception {
    kmp = new KMeansParallel(r);
  }
  
  @Test
  public void testBasic() throws Exception {
    Vector initialPoint = Vectors.of(1.0, 1.0);
    KMeans km = new KMeans();
    
    List<List<Weighted<Vector>>> points = kmp.initialization(vecs, 5, 4, initialPoint,
        new Crossfold(2, 1729L));
    List<Centers> centers = Lists.newArrayList();
    List<Weighted<Vector>> allPoints = Lists.newArrayList(points.get(0));
    allPoints.addAll(points.get(1));
    centers.add(km.compute(allPoints, 1, new Random(17)));
    centers.add(km.compute(allPoints, 2, new Random(17)));
    centers.add(km.compute(allPoints, 3, new Random(17)));
    List<Double> costs = kmp.getCosts(vecs, centers).getValue();
    assertEquals(ImmutableList.of(67.0, 6.0, 4.0), costs);
  }
}
