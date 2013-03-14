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
package com.cloudera.science.ml.kmeans.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.Centers;
import com.cloudera.science.ml.core.Weighted;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * An in-memory implementation of the k-means algorithm (also known as Lloyd's algorithm)
 * that can be configured to create various numbers of clusters using different
 * {@link KMeansInitStrategy} initialization strategies and terminating based on
 * different {@code StoppingCriteria} rules. For more details on the implementation and
 * its properties, please see <a href="http://en.wikipedia.org/wiki/K-means_clustering">the
 * Wikipedia page.</a>
 */
public class KMeans {

  private final KMeansInitStrategy initStrategy;
  private final StoppingCriteria stoppingCriteria;
  
  /**
   * Constructor that uses the k-means++ initialization strategy and
   * a 1000-iteration stopping criteria.
   * 
   * @param numClusters The number of clusters to create
   * @param stoppingCriteria The stopping criteria to use for Lloyd's algorithm
   */
  public KMeans() {
    this(KMeansInitStrategy.PLUS_PLUS, StoppingCriteria.threshold(1000));
  }
  
  /**
   * Creates an in-memory k-means execution engine.
   * 
   * @param initStrategy The initialization strategy for the k-means algorithm
   * @param stoppingCriteria The stopping criteria to use for Lloyd's algorithm
   */
  public KMeans(
      KMeansInitStrategy initStrategy,
      StoppingCriteria stoppingCriteria) {
    this.initStrategy = Preconditions.checkNotNull(initStrategy);
    this.stoppingCriteria = Preconditions.checkNotNull(stoppingCriteria);
  }
  
  /**
   * Apply the configured k-means initialization strategy followed by
   * Lloyd's algorithm for the given list of {@code WeightedVec} instances.
   * 
   * @param points The weighted points to cluster
   * @return The {@code Centers} created from the computations
   */
  public <V extends Vector> Centers compute(List<Weighted<V>> points, int numClusters) {
    return compute(points, numClusters, null);
  }

  /**
   * Apply the configured k-means initialization strategy followed by
   * Lloyd's algorithm for the given list of {@code WeightedVec} instances.
   * 
   * @param points The weighted points to cluster
   * @param random The random number generator to use
   * @return The {@code Centers} created from the computations
   */
  public <V extends Vector> Centers compute(List<Weighted<V>> points, int numClusters, Random random) {
    Preconditions.checkArgument(numClusters > 0);
    Centers c = initStrategy.apply(points, numClusters, random);
    return lloydsAlgorithm(points, c);
  }

  /**
   * Apply Lloyd's algorithm to the given points and centers until the stopping
   * criteria is met.
   * 
   * @param points The weighted points
   * @param centers The initial centers
   * @return The centers that the algorithm converged toward
   */
  public <V extends Vector> Centers lloydsAlgorithm(Collection<Weighted<V>> points, Centers centers) {
    Centers current = centers, last = null;
    int iteration = 0;
    while (!stoppingCriteria.stop(iteration, current, last)) {
      last = current;
      current = updateCenters(points, last);
      iteration++;
    }
    return current;
  }
  
  /**
   * Performs a single update cycle of Lloyd's algorithm.
   * 
   * @param points The weighted points
   * @param centers The current centers
   * @return The new centers computed by the update
   */
  public <V extends Vector> Centers updateCenters(Collection<Weighted<V>> points, Centers centers) {
    Map<Integer, List<Weighted<V>>> assignments = Maps.newHashMap();
    for (int i = 0; i < centers.size(); i++) {
      assignments.put(i, Lists.<Weighted<V>>newArrayList());
    }
    for (Weighted<V> weightedVec : points) {
      assignments.get(centers.indexOfClosest(weightedVec.thing())).add(weightedVec);
    }
    List<Vector> centroids = Lists.newArrayList();
    for (List<Weighted<V>> assigned : assignments.values()) {
      centroids.add(centroid(assigned));
    }
    return new Centers(centroids);
  }
  
  /**
   * Compute the {@code Vector} that is the centroid of the given weighted points.
   * 
   * @param points The weighted points
   * @return The centroid of the weighted points
   */
  public <V extends Vector> Vector centroid(Collection<Weighted<V>> points) {
    Vector center = null;
    long sz = 0;
    for (Weighted<V> v : points) {
      Vector weighted = v.thing().times(v.weight());
      if (center == null) {
        center = weighted;
      } else {
        center = center.plus(weighted);
      }
      sz += v.weight();
    }
    return center.divide(sz);
  }  
}
