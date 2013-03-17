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
public class PredictionStrength {

  private final List<Centers> testCenters;
  private final List<Weighted<Vector>> testPoints;
  private final List<Centers> trainCenters;
  
  public PredictionStrength(List<Centers> testCenters, List<Weighted<Vector>> testPoints,
      List<Centers> trainCenters) {
    this.testCenters = testCenters;
    this.testPoints = testPoints;
    this.trainCenters = trainCenters;
  }
  
  public List<Double> computeScores() {
    List<Double> scores = Lists.newArrayListWithExpectedSize(testCenters.size());
    for (int i = 0; i < testCenters.size(); i++) {
      Centers test = testCenters.get(i);
      Centers train = trainCenters.get(i);
      double[][] assignments = new double[test.size()][train.size()];
      for (Weighted<Vector> wv : testPoints) {
        int testId = test.indexOfClosest(wv.thing());
        int trainId = train.indexOfClosest(wv.thing());
        assignments[testId][trainId] += wv.weight();
      }
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
      scores.add(minScore);
    }
    return scores;
  }
}
