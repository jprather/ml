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
package com.cloudera.science.ml.parallel.types;


import org.apache.crunch.MapFn;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.avro.MLVector;
import com.cloudera.science.ml.core.MLConvert;


/**
 * Factory methods for creating {@code PType} instances for use with the ML Parallel libraries.
 */
public class MLAvros {

  /**
   * Returns an {@code AvroType<Vector>} based on the {@code AvroTypeFamily}.
   */
  public static AvroType<Vector> vector() {
    return vector;
  }
  
  public static AvroType<NamedVector> namedVector() {
    return namedVector;
  }
  
  private static AvroType<Vector> vector = Avros.derived(Vector.class,
      new MapFn<MLVector, Vector>() {
        @Override
        public Vector map(MLVector vec) {
          return MLConvert.toVector(vec);
        }
      },
      new MapFn<Vector, MLVector>() {
        @Override
        public MLVector map(Vector vec) {
          return MLConvert.fromVector(vec);
        }
      },
      Avros.specifics(MLVector.class));
  
  private static AvroType<NamedVector> namedVector = Avros.derived(NamedVector.class,
      new MapFn<MLVector, NamedVector>() {
        @Override
        public NamedVector map(MLVector vec) {
          return (NamedVector) MLConvert.toVector(vec);
        }
      },
      new MapFn<NamedVector, MLVector>() {
        @Override
        public MLVector map(NamedVector vec) {
          return MLConvert.fromVector(vec);
        }
      },
      Avros.specifics(MLVector.class));
  
  static {
    Avros.register(Vector.class, vector);
    Avros.register(NamedVector.class, namedVector);
  }

  private MLAvros() {}
}
