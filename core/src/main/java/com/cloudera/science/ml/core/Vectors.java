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
package com.cloudera.science.ml.core;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * Factory methods for creating new {@code Vector} objects.
 */
public class Vectors {

  public static double[] toArray(Vector v) {
    double[] ret = new double[v.size()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = v.getQuick(i);
    }
    return ret;
  }
  
  public static Vector of(double... values) {
    return new DenseVector(values);
  }
  
  public static Vector dense(int size) {
    double[] d = new double[size];
    return new DenseVector(d);
  }
  
  public static Vector sparse(int size) {
    return new RandomAccessSparseVector(size);
  }

  public static Vector named(String name, double... v) {
    return new NamedVector(of(v), name);
  }
  
  private Vectors() {}
}
