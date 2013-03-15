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
package com.cloudera.science.ml.core.vectors;

import static org.junit.Assert.assertEquals;

import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.Vectors;

public class CentersTest {
  private static double THRESH = 0.001;
  
  Vector a = Vectors.of(17.0, 29.0);
  Vector b = Vectors.of(18.0, 27.0);
  Vector c = Vectors.of(16.0, 25.0);
  
  @Test
  public void testSingleton() throws Exception {
    Centers centers = new Centers(a);
    assertEquals(0.0, centers.getDistanceSquared(a), THRESH);
    assertEquals(5.0, centers.getDistanceSquared(b), THRESH);
    assertEquals(17.0, centers.getDistanceSquared(c), THRESH);
    assertEquals(0, centers.indexOfClosest(b));
  }
  
  @Test
  public void testTwo() throws Exception {
    Centers centers = new Centers(a, b);
    assertEquals(0.0, centers.getDistanceSquared(a), THRESH);
    assertEquals(0.0, centers.getDistanceSquared(b), THRESH);
    assertEquals(8.0, centers.getDistanceSquared(c), THRESH);
    assertEquals(1, centers.indexOfClosest(c));
    assertEquals(0, centers.indexOfClosest(a));
  }
}
