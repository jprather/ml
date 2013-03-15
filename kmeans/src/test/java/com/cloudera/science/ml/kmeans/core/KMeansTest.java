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
package com.cloudera.science.ml.kmeans.core;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Random;

import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.google.common.collect.ImmutableList;

public class KMeansTest {

  Weighted<Vector> a = wpoint(1.0, 1.0);
  Weighted<Vector> b = wpoint(5.0, 4.0);
  Weighted<Vector> c = wpoint(4.0, 3.0);
  Weighted<Vector> d = wpoint(2.0, 1.0);
  List<Weighted<Vector>> points = ImmutableList.of(a, b, c, d);
  
  KMeans kmeans = new KMeans();
  private Random rand;
  
  @Before
  public void setUp() throws Exception {
    rand = new Random(1729L);
  }
  
  public Vector vec(double... values) {
    return Vectors.of(values);
  }
  
  public Weighted<Vector> wpoint(double... values) {
    return new Weighted<Vector>(vec(values));
  }
  
  @Test
  public void testCentroids() throws Exception {
    assertEquals(vec(3.0, 2.5), kmeans.centroid(ImmutableList.of(a, b)));
    assertEquals(vec(3.0, 2.0), kmeans.centroid(ImmutableList.of(c, d))); 
    assertEquals(vec(1.0, 1.0), kmeans.centroid(ImmutableList.of(a)));
  }
  
  @Test
  public void testUpdate() throws Exception {
    Centers centers = new Centers(a.thing(), b.thing());
    Centers expected = new Centers(vec(1.5, 1.0), vec(4.5, 3.5));
    assertEquals(expected, kmeans.updateCenters(points, centers));
  }
  
  @Test
  public void testConvergence() throws Exception {
    Centers centers = new Centers(a.thing(), b.thing());
    Centers converged = kmeans.lloydsAlgorithm(points, centers);
    Centers expected = new Centers(vec(1.5, 1.0), vec(4.5, 3.5));
    assertEquals(expected, converged);
  }

  @Test
  public void testRandomInit() throws Exception {
    Centers expected = new Centers(vec(4.0, 3.0), vec(2.0, 1.0));
    assertEquals(expected, KMeansInitStrategy.RANDOM.apply(points, 2, rand));
    
    Centers done = kmeans.lloydsAlgorithm(points, expected);
    assertEquals(new Centers(vec(4.5, 3.5), vec(1.5, 1.0)), done);
  }
  
  @Test
  public void testPlusPlusInit() throws Exception {
    Centers expected = new Centers(vec(2.0, 1.0), vec(4.0, 3.0));
    assertEquals(expected, KMeansInitStrategy.PLUS_PLUS.apply(points, 2, rand));
    
    Centers done = kmeans.lloydsAlgorithm(points, expected);
    assertEquals(new Centers(vec(1.5, 1.0), vec(4.5, 3.5)), done);
  }
  
}
