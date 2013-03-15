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


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.avro.MLVector;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.avro.AvroRecord;
import com.cloudera.science.ml.core.vectors.VectorConvert;


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
  
  public static AvroType<Record> record(Schema schema) {
    return Avros.derived(Record.class,
        new MapFn<GenericData.Record, Record>() {
          @Override
          public Record map(GenericData.Record gdr) {
            return new AvroRecord(gdr);
          }
        },
        new MapFn<Record, GenericData.Record>() {
          @Override
          public GenericData.Record map(Record r) {
            return ((AvroRecord) r).getImpl();
          }
        },
        Avros.generics(schema));
  }
  
  private static AvroType<Vector> vector = Avros.derived(Vector.class,
      new MapFn<MLVector, Vector>() {
        @Override
        public Vector map(MLVector vec) {
          return VectorConvert.toVector(vec);
        }
      },
      new MapFn<Vector, MLVector>() {
        @Override
        public MLVector map(Vector vec) {
          return VectorConvert.fromVector(vec);
        }
      },
      Avros.specifics(MLVector.class));
  
  private static AvroType<NamedVector> namedVector = Avros.derived(NamedVector.class,
      new MapFn<MLVector, NamedVector>() {
        @Override
        public NamedVector map(MLVector vec) {
          return (NamedVector) VectorConvert.toVector(vec);
        }
      },
      new MapFn<NamedVector, MLVector>() {
        @Override
        public MLVector map(NamedVector vec) {
          return VectorConvert.fromVector(vec);
        }
      },
      Avros.specifics(MLVector.class));
  
  static {
    Avros.register(Vector.class, vector);
    Avros.register(NamedVector.class, namedVector);
  }

  private MLAvros() {}
}
