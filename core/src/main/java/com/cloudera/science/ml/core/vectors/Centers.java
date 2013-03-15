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

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import org.apache.mahout.math.Vector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Represents a collection of {@code Vector} instances that act as the centers of
 * a set of clusters, as in k-means models.
 */
public class Centers extends AbstractList<Vector> {
  // The vectors, where each vector is the center of a particular cluster
  private final List<Vector> centers;
  
  /**
   * Create a new instance from the given points. Any duplicate
   * points in the arg list will be removed.
   * 
   * @param points The points
   * @throws IllegalArgumentException if no points are given
   */
  public Centers(Vector... points) {
    this(Arrays.asList(points));
  }
  
  /**
   * Create a new instance from the given points. Any duplicate
   * points in the {@code Iterable} instance will be removed.
   * 
   * @param points The points
   * @throws IllegalArgumentException if the input is empty
   */
  public Centers(Iterable<Vector> points) {
    this.centers = ImmutableList.copyOf(Sets.newLinkedHashSet(points));
    Preconditions.checkArgument(this.centers.size() > 0);
  }
  
  /**
   * Returns the number of points in this instance.
   */
  public int size() {
    return centers.size();
  }
  
  /**
   * Returns the {@code Vec} at the given index.
   */
  public Vector get(int index) {
    return centers.get(index);
  }

  /**
   * Construct a new {@code Centers} object made up of the given {@code Vec}
   * and the points contained in this instance.
   * 
   * @param point The new point
   * @return A new {@code Centers} instance
   */
  public Centers extendWith(Vector point) {
    return new Centers(Iterables.concat(centers, ImmutableList.of(point)));
  }
  
  /**
   * Construct a new {@code Centers} object made up of the given points
   * and the points contained in this instance.
   * 
   * @param points The new points
   * @return A new {@code Centers} instance
   */
  public Centers extendWith(Iterable<Vector> points) {
    return new Centers(Iterables.concat(centers, points));
  }
  
  /**
   * Returns the minimum squared Euclidean distance between the given
   * {@code Vec} and a point contained in this instance.
   * 
   * @param point The point
   * @return The minimum squared Euclidean distance from the point 
   */
  public double getDistanceSquared(Vector point) {
    double min = Double.POSITIVE_INFINITY;
    for (Vector c : centers) {
      min = Math.min(min, c.getDistanceSquared(point));
    }
    return min;
  }
  
  /**
   * Returns the index of the {@code Vec} within this instance that is
   * closest to the given {@code Vec}.
   * 
   * @param point The point
   * @return The index of the closest {@code Vec} to the given point
   */
  public int indexOfClosest(Vector point) {
    int index = -1;
    double min = Double.POSITIVE_INFINITY;
    for (int i = 0; i < centers.size(); i++) {
      double d = centers.get(i).getDistanceSquared(point); 
      if (d < min) {
        min = d;
        index = i;
      }
    }
    return index;
  }
  
  /**
   * Calculate the sum of the element-wise squared distances between this
   * instance and the given {@code Centers}.
   * 
   * @param other The other points
   * @return The sum of the squared distances
   */
  public double getSumOfSquaredDistances(Centers other) {
    Preconditions.checkArgument(size() == other.size());
    double sum = 0.0;
    for (int i = 0; i < centers.size(); i++) {
      sum += centers.get(i).getDistanceSquared(other.centers.get(i));
    }
    return sum;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Centers)) {
      return false;
    }
    Centers c = (Centers) other;
    return centers.containsAll(c.centers) && c.centers.containsAll(centers);
  }
  
  @Override
  public int hashCode() {
    int hc = 0;
    for (int i = 0; i < centers.size(); i++) {
      hc += centers.get(i).hashCode();
    }
    return hc;
  }
  
  @Override
  public String toString() {
    return centers.toString();
  }
}
