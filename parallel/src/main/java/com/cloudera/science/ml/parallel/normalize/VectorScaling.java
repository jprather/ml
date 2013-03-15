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

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.vectors.Vectors;

/**
 * Functions for applying scale factors to a {@code PCollection<Vector>} instance, which is
 * useful if we want to weight some features more than others for clustering.
 */
public class VectorScaling {

  public static <V extends Vector> PCollection<V> scale(PCollection<V> vecs, double[] scaleFactors) {
    return vecs.parallelDo("scale", new ScaleFn<V>(scaleFactors), vecs.getPType());
  }
  
  private static class ScaleFn<V extends Vector> extends MapFn<V, V> {
    private final double[] scaleFactors;
    private transient Vector scaleVec;
    
    public ScaleFn(double[] scaleFactors) {
      this.scaleFactors = scaleFactors;
    }

    @Override
    public void initialize() {
      this.scaleVec = Vectors.of(scaleFactors);
    }
    
    @Override
    public V map(V v) {
      return (V) v.times(scaleVec);
    } 
  }
}
