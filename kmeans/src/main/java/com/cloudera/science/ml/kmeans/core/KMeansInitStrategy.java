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

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.google.common.collect.Sets;

/**
 * The strategies used to choose the initial {@link Centers} used for a k-means algorithm
 * run prior to running Lloyd's algorithm.
 */
public enum KMeansInitStrategy {

  /**
   * Uses the classic random selection strategy to create the initial {@code Centers}. The
   * algorithm will randomly choose K points from the input points, favoring points with
   * higher weights.
   */
  RANDOM {
    @Override
    public <V extends Vector> Centers apply(List<Weighted<V>> points, int numClusters, Random random) {
      if (random == null) {
        random = new Random();
      }
      Set<Vector> candidates = Sets.newHashSet();
      double[] cumulativeSum = cumulativeSum(points);
      double max = cumulativeSum[cumulativeSum.length - 1];
      while (candidates.size() < numClusters) {
        double offset = max * random.nextDouble();
        int next = Arrays.binarySearch(cumulativeSum, offset);
        Weighted<V> wv = (next >= 0) ? points.get(next - 1) : points.get(-2 - next);
        candidates.add(wv.thing());
      }
      return new Centers(candidates);
    }
    
    private <V extends Vector> double[] cumulativeSum(List<Weighted<V>> points) {
      double[] sums = new double[points.size() + 1];
      for (int i = 0; i < sums.length - 1; i++) {
        sums[i + 1] = sums[i] + points.get(i).weight();
      }
      return sums;
    }
  },
  
  /**
   * Uses the <i>k-means++</i> strategy described in Arthur and Vassilvitskii (2007).
   * See <a href="http://en.wikipedia.org/wiki/K-means%2B%2B">the Wikipedia page</a>
   * for details.
   */
  PLUS_PLUS {
    @Override
    public <V extends Vector> Centers apply(List<Weighted<V>> points, int numClusters, Random random) {
      if (random == null) {
        random = new Random();
      }
      Centers centers = RANDOM.apply(points, 1, random);
      double[] cumulativeScores = new double[points.size() + 1];
      for (int i = 1; i < numClusters; i++) {
        cumulativeScores[0] = 0;
        for (int j = 0; j < points.size(); j++) {
          Weighted<V> wv = points.get(j);
          double score = centers.getDistanceSquared(wv.thing()) * wv.weight();
          cumulativeScores[j + 1] = cumulativeScores[j] + score;
        }
        double r = cumulativeScores[points.size()] * random.nextDouble();
        int next = Arrays.binarySearch(cumulativeScores, r);
        int index = (next > 0) ? next - 1 : -2 - next;
        while (index > 0 && centers.contains(points.get(index).thing())) {
          index--;
        }
        centers = centers.extendWith(points.get(index).thing());
      }
      return centers;
    }
  };

  /**
   * Use this instance to create the initial {@code Centers} from the given parameters.
   * 
   * @param points The candidate {@code WeightedVec} instances for the cluster
   * @param numClusters The number of points in the center (i.e., the "k" in "k-means")
   * @param random The {@code Random} to use
   * @return A new {@code Centers} instance created using this instance
   */
  public abstract <V extends Vector> Centers apply(List<Weighted<V>> points, int numClusters, Random random);
}
