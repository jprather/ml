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
import com.google.common.base.Function;
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
    return Lists.transform(testCosts, new Function<Double, Double>() {
      @Override
      public Double apply(Double input) {
        return input / testCosts.get(0);
      }
    });
  }
  
  public List<Double> getTrainCosts() {
    return Lists.transform(trainCosts, new Function<Double, Double>() {
      @Override
      public Double apply(Double input) {
        return input / trainCosts.get(0);
      }
    });
  }
  
  private void init() {
    predictionStrengths = Lists.newArrayListWithExpectedSize(testCenters.size());
    trainCosts = Lists.newArrayListWithExpectedSize(testCenters.size());
    testCosts = Lists.newArrayListWithExpectedSize(testCenters.size());
    
    for (int i = 0; i < testCenters.size(); i++) {
      Centers test = testCenters.get(i);
      Centers train = trainCenters.get(i);
      double trainCost = 0.0, testCost = 0.0;
      double[][] assignments = new double[test.size()][train.size()];
      for (Weighted<Vector> wv : testPoints) {
        double wt = wv.weight();
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
      for (double[] assignment : assignments) {
        double total = 0.0;
        double same = 0.0;
        for (double a : assignment) {
          total += a;
          same += a * (a - 1);
        }
        double score = same / (total * (total - 1));
        if (score < minScore) {
          minScore = score;
        }
      }
      predictionStrengths.add(minScore);
    }
  }
}
