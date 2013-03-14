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
package com.cloudera.science.ml.mahout.types;

import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.Writables;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Factory methods for creating {@code PType} instances for use with the ML Parallel libraries.
 */
public class MLWritables {

  static {
    Writables.register(Vector.class, (WritableType) vector());
    Writables.register(NamedVector.class, (WritableType) vector(NamedVector.class));
  }
  
  /**
   * Returns a {@code PType<Vector>} based on the {@code WritableTypeFamily}.
   */
  public static PType<Vector> vector() {
    return vector(Vector.class);
  }
  
  public static <V extends Vector> PType<V> vector(Class<V> vectorClass) {
      return Writables.derived(vectorClass,
          (MapFn<VectorWritable, V>) VEC_IN,
          (MapFn<V, VectorWritable>) VEC_OUT,
          Writables.writables(VectorWritable.class));
  }
  
  private static MapFn<VectorWritable, Vector> VEC_IN = new MapFn<VectorWritable, Vector>() {
    @Override
    public Vector map(VectorWritable input) {
      return input.get();
    }
  };

  private static MapFn<Vector, VectorWritable> VEC_OUT = new MapFn<Vector, VectorWritable>() {
    @Override
    public VectorWritable map(Vector input) {
      return new VectorWritable(input);
    }
  };

  private MLWritables() { }
}
