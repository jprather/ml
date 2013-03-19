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
package com.cloudera.science.ml.kmeans.core;

import java.util.List;

import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.google.common.collect.Lists;

/**
 * A strategy for evaluating different choices of K based on how well the points
 * in a dataset overlap between clusters that were created from different subsets
 * of the data.
 */
public class KMeansEvaluation {

  private final List<Centers> testCenters;
  private final List<Weighted<Vector>> testPoints;
  private final List<Centers> trainCenters;
  private List<Double> predictionStrengths;
  private List<Double> trainCosts;
  private List<Double> testCosts;
  private List<Double> stablePoints;
  private List<Double> stableClusters;
  
  public KMeansEvaluation(List<Centers> testCenters, List<Weighted<Vector>> testPoints,
      List<Centers> trainCenters) {
    this.testCenters = testCenters;
    this.testPoints = testPoints;
    this.trainCenters = trainCenters;
    init();
  }
  
  public List<Double> getPredictionStrengths() {
    return predictionStrengths;
  }
  
  public List<Double> getTestCenterCosts() {
    return testCosts;
  }
  
  public List<Double> getTrainCosts() {
    return trainCosts;
  }
  
  public List<Double> getStableClusters() {
    return stableClusters;
  }
  
  public List<Double> getStablePoints() {
    return stablePoints;
  }
  
  private void init() {
    predictionStrengths = Lists.newArrayListWithExpectedSize(testCenters.size());
    trainCosts = Lists.newArrayListWithExpectedSize(testCenters.size());
    testCosts = Lists.newArrayListWithExpectedSize(testCenters.size());
    stableClusters = Lists.newArrayListWithExpectedSize(testCenters.size());
    stablePoints = Lists.newArrayListWithExpectedSize(testCenters.size());
    
    for (int i = 0; i < testCenters.size(); i++) {
      Centers test = testCenters.get(i);
      Centers train = trainCenters.get(i);
      double trainCost = 0.0, testCost = 0.0;
      double[][] assignments = new double[test.size()][train.size()];
      int totalPoints = 0;
      for (Weighted<Vector> wv : testPoints) {
        double wt = wv.weight();
        totalPoints += wt;
        Vector v = wv.thing();
        int testId = test.indexOfClosest(v);
        testCost += wt * v.getDistanceSquared(test.get(testId));
        int trainId = train.indexOfClosest(wv.thing());
        trainCost += wt * v.getDistanceSquared(train.get(trainId));
        assignments[testId][trainId] += wt;
      }
      trainCosts.add(trainCost);
      testCosts.add(testCost);
      
      double minScore = Double.POSITIVE_INFINITY;
      double points = 0;
      double clusters = 0;
      for (double[] assignment : assignments) {
        double total = 0.0;
        double same = 0.0;
        for (double a : assignment) {
          total += a;
          same += a * (a - 1);
        }
        double score = same / (total * (total - 1));
        // Only consider clusters that contain a non-trivial number of obs
        if (total > assignment.length && score < minScore) {
          minScore = score;
        }
        if (score > 0.8) { // stability threshold
          clusters++;
          points += total;
        }
      }
      predictionStrengths.add(minScore);
      stableClusters.add(clusters / assignments.length);
      stablePoints.add(points / totalPoints);
    }
  }
}
