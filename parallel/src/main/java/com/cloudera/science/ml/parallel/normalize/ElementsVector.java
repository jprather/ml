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
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.vectors.Vectors;

/**
 *
 */
public class ElementsVector extends Elements {

  private final Vector vec;
  
  public static PCollection<Elements> convert(PCollection<Vector> vectors) {
    return vectors.parallelDo(new MapFn<Vector, Elements>() {
      @Override
      public Elements map(Vector vec) {
        return new ElementsVector(vec);
      } 
    }, ptype(vectors.getPType()));
  }
  
  public ElementsVector(Vector vec) {
    this.vec = vec;
  }
  
  @Override
  public Vector asVector(double[] values, boolean sparse) {
    // TODO: handle sparse arg
    return vec.like().assign(values);
  }
  
  @Override
  public Element get(int index) {
    return new Element(index, vec.getQuick(index));
  }

  @Override
  public int size() {
    return vec.size();
  }

  private static final MapFn<Vector, Elements> IN = new MapFn<Vector, Elements>() {
    @Override
    public Elements map(Vector in) {
      return new ElementsVector(in);
    }
  };
  
  private static final MapFn<Elements, Vector> OUT = new MapFn<Elements, Vector>() {
    @Override
    public Vector map(Elements elems) {
      if (elems instanceof ElementsVector) {
        return ((ElementsVector) elems).vec;
      } else {
        double[] values = new double[elems.size()];
        for (int i = 0; i < values.length; i++) {
          Element e = elems.get(i);
          values[e.index()] = e.getNumeric();
        }
        return Vectors.of(values);
      }
    }
  };

  public static PType<Elements> ptype(PType<Vector> vectorPType) {
    PTypeFamily ptf = vectorPType.getFamily();
    return ptf.derived(Elements.class, IN, OUT, vectorPType);
  }
}
