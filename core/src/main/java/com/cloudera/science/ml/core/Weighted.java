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

import java.util.Collection;
import java.util.List;

import org.apache.mahout.math.Vector;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;

/**
 * A container for a {@link Vector} and an associated numeric weight. {@code WeightedVec}
 * instances are used to represent a summary of the distribution of points in a much larger
 * data set.
 */
public class Weighted<T> {
  private final T thing;
  private final double weight;
  
  private static class WeightFunction<T> implements Function<T, Weighted<T>> {
    @Override
    public Weighted<T> apply(T input) {
      return new Weighted<T>(input);
    }
  }
  
  /**
   * Converts an input {@code Collection<Vector>} into a {@code Collection<WeightedVec>}.
   * 
   * @param vecs The points to convert
   * @return The points as {@code WeightedVec} values with weight 1.0
   */
  public static <T> Collection<Weighted<T>> create(Collection<T> things) {
    return Collections2.transform(things, new WeightFunction<T>());
  }
  
  /**
   * Converts an input {@code List<Vector>} into a {@code List<WeightedVec>}.
   * 
   * @param things The points to convert
   * @return The points as {@code WeightedVec} values with weight 1.0
   */
  public static <T> List<Weighted<T>> create(List<T> things) {
    return Lists.transform(things, new WeightFunction<T>());
  }
  
  /**
   * Create a new instance with weight 1.0.
   * 
   * @param thing The thing that is weighted
   */
  public Weighted(T thing) {
    this(thing, 1.0);
  }
  
  /**
   * Create a new instance with the given weight.
   * 
   * @param thing The thing to weight
   * @param weight The weight
   */
  public Weighted(T thing, double weight) {
    this.thing = thing;
    this.weight = weight;
  }
  
  /**
   * Return the thing referenced by this instance.
   */
  public T thing() {
    return thing;
  }
  
  /**
   * Return the numeric weight for this instance.
   */
  public double weight() {
    return weight;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Weighted)) {
      return false;
    }
    Weighted<T> wv = (Weighted<T>) other;
    return thing.equals(wv.thing) && weight == wv.weight;
  }
  
  @Override
  public int hashCode() {
    return 17 * thing.hashCode() + 37 * Doubles.hashCode(weight);
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append(thing)
        .append(";")
        .append(weight)
        .toString();
  }
}
