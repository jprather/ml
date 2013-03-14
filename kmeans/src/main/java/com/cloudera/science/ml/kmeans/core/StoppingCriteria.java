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

import com.cloudera.science.ml.core.Centers;
import com.google.common.base.Preconditions;

/**
 * Represents the rule used by Lloyd's algorithm to decide that a particular
 * set of {@link Centers} has converged.
 */
public abstract class StoppingCriteria {

  /**
   * Stop when the sum of squared distances between the points in the current and
   * last {@code Centers} is less than the given threshold.
   * 
   * @param threshold The threshold
   * @return A new {@code StoppingCriteria}
   * @throws IllegalArgumentException if the threshold is <= 0.0
   */
  public static StoppingCriteria threshold(double threshold) {
    return new ThresholdStoppingCriteria(threshold);
  }
  
  /**
   * Stop when the number of iterations has exceeded a maximum, regardless of
   * the differences between the current and the last {@code Centers}.
   * 
   * @param maxIterations The maximum number of iterations to run
   * @return A new {@code StoppingCriteria}
   * @throws IllegalArgumentException if the maxIterations < 0
   */
  public static StoppingCriteria maxIterations(int maxIterations) {
    return new MaxIterationStoppingCriteria(maxIterations);
  }
  
  /**
   * Construct composite {@code StoppingCriteria} by performing the logical OR of
   * other {@code StoppingCriteria} instances.
   * 
   * @param criteria The {@code StoppingCriteria}
   * @return A new {@code StoppingCriteria}
   * @throws IllegalArgumentException if no criteria are provided
   */
  public static StoppingCriteria or(StoppingCriteria... criteria) {
    return new OrStoppingCriteria(criteria); 
  }
  
  /**
   * Returns true if the rules for this instance are met, based on the
   * current {@code Centers} and most recent ("last") {@code Centers} computed
   * by Lloyd's algorithm.
   * 
   * @param iteration The current iteration of Lloyd's algorithm
   * @param current The current {@code Centers}
   * @param last The most recent {@code Centers} before the current
   * @return True if the iterations should stop
   */
  public abstract boolean stop(int iteration, Centers current, Centers last);


  private static class ThresholdStoppingCriteria extends StoppingCriteria {
    private final double threshold;
    
    public ThresholdStoppingCriteria(double threshold) {
      Preconditions.checkArgument(threshold > 0);
      this.threshold = threshold;
    }
    
    @Override
    public boolean stop(int iteration, Centers current, Centers last) {
      if (last == null) {
        return false;
      }
      return last.getSumOfSquaredDistances(current) < threshold;
    }
  }
  
  private static class MaxIterationStoppingCriteria extends StoppingCriteria {
    private final int maxIterations;

    public MaxIterationStoppingCriteria(int maxIterations) {
      Preconditions.checkArgument(maxIterations > 0);
      this.maxIterations = maxIterations;
    }
    
    @Override
    public boolean stop(int iteration, Centers current, Centers last) {
      return iteration >= maxIterations;
    }
  }
  
  private static class OrStoppingCriteria extends StoppingCriteria {
    private StoppingCriteria[] criteria;
    
    public OrStoppingCriteria(StoppingCriteria[] criteria) {
      Preconditions.checkArgument(criteria.length > 0);
      this.criteria = criteria;
    }
    
    @Override
    public boolean stop(int iteration, Centers current, Centers last) {
      for (StoppingCriteria sc : criteria) {
        if (sc.stop(iteration, current, last)) {
          return true;
        }
      }
      return false;
    }
  }
}
