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
package com.cloudera.science.ml.core;

import java.util.Iterator;
import java.util.List;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.avro.MLCenters;
import com.cloudera.science.ml.avro.MLVector;
import com.cloudera.science.ml.avro.MLWeightedVector;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Utilities for converting from the Avro and the Java implementations of the core ML type classes.
 */
public final class MLConvert {

  public static Vector toVector(MLVector input) {
    return TO_VECTOR.apply(input);
  }
  
  public static MLVector fromVector(Vector input) { 
    return FROM_VECTOR.apply(input);
  }
  
  public static Weighted<Vector> toWeightedVec(MLWeightedVector input) {
    return TO_WEIGHTED_VEC.apply(input);
  }
  
  public static MLWeightedVector fromWeightedVec(Weighted<Vector> input) {
    return FROM_WEIGHTED_VEC.apply(input);
  }

  public static Centers toCenters(MLCenters input) {
    return TO_CENTERS.apply(input);
  }
  
  public static MLCenters fromCenters(Centers input) {
    return FROM_CENTERS.apply(input);
  }
  
  public static Function<MLVector, Vector> TO_VECTOR = new Function<MLVector, Vector>() {
    @Override
    public Vector apply(MLVector input) {
      Vector base = null;
      if (input.getIndices().isEmpty()) {
        double[] d = new double[input.getSize()];
        for (int i = 0; i < d.length; i++) {
          d[i] = input.getValues().get(i);
        }
        base = new DenseVector(d);
      } else {
        List<Double> values = input.getValues();
        base = new RandomAccessSparseVector(input.getSize(), values.size());
        for (int i = 0; i < values.size(); i++) {
          base.set(input.getIndices().get(i), values.get(i));
        }
      }
      if (input.getId().length() > 0) {
        base = new NamedVector(base, input.getId().toString());
      }
      return base;
    }
  };
  
  public static Function<Vector, MLVector> FROM_VECTOR = new Function<Vector, MLVector>() {
    @Override
    public MLVector apply(Vector input) {
      List<Double> values = Lists.newArrayList();
      MLVector.Builder vb = MLVector.newBuilder()
          .setSize(input.size())
          .setValues(values);
      if (input.isDense()) {
        vb.setIndices(ImmutableList.<Integer>of());
        for (int i = 0; i < input.size(); i++) {
          values.add(input.getQuick(i));
        }
      } else {
        List<Integer> indices = Lists.newArrayList();
        vb.setIndices(indices);
        Iterator<Vector.Element> iter = input.iterateNonZero();
        while (iter.hasNext()) {
          Vector.Element e = iter.next();
          indices.add(e.index());
          values.add(e.get());
        }
      }
      if (input instanceof NamedVector) {
        vb.setId(((NamedVector) input).getName());
      } else {
        vb.setId("");
      }
      return vb.build();
    }
  };

  public static Function<MLWeightedVector, Weighted<Vector>> TO_WEIGHTED_VEC = new Function<MLWeightedVector, Weighted<Vector>>() {
    @Override
    public Weighted<Vector> apply(MLWeightedVector input) {
      return new Weighted<Vector>(TO_VECTOR.apply(input.getVec()), input.getWeight());
    }
  };
  
  public static Function<Weighted<Vector>, MLWeightedVector> FROM_WEIGHTED_VEC = new Function<Weighted<Vector>, MLWeightedVector>() {
    @Override
    public MLWeightedVector apply(Weighted<Vector> input) {
      MLWeightedVector.Builder b = MLWeightedVector.newBuilder();
      b.setVec(FROM_VECTOR.apply(input.thing())).setWeight(input.weight());
      return b.build();
    }
  };
  
  public static Function<MLCenters, Centers> TO_CENTERS = new Function<MLCenters, Centers>() {
    @Override
    public Centers apply(MLCenters input) {
      return new Centers(Lists.transform(input.getCenters(), TO_VECTOR));
    }
  };
  
  public static Function<Centers, MLCenters> FROM_CENTERS = new Function<Centers, MLCenters>() {
    @Override
    public MLCenters apply(Centers input) {
      MLCenters.Builder b = MLCenters.newBuilder();
      b.setCenters(Lists.transform(input, FROM_VECTOR));
      return b.build();
    }
  };
  
  private MLConvert() {}
}
