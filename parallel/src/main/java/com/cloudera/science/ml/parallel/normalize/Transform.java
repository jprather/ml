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

import java.io.Serializable;

import com.cloudera.science.ml.parallel.summary.SummaryStats;

/**
 *
 */
public interface Transform extends Serializable {
  
  double apply(double value, long recordCount, SummaryStats stats);

  public static final Transform NONE = new Transform() {
    public double apply(double value, long recordCount, SummaryStats stats) {
      return value;
    }
  };
  
  public static final Transform Z = new Transform() {
    public double apply(double value, long recordCount,  SummaryStats stats) {
      if (stats.stdDev(recordCount) == 0.0) {
        return value;
      }
      return (value - stats.mean(recordCount)) / stats.stdDev(recordCount);
    }
  };
  
  public static final Transform LINEAR = new Transform() {
    public double apply(double value, long recordCount, SummaryStats stats) {
      if (stats.range() == 0.0) {
        return value;
      }
      return (value - stats.min()) / (stats.range());
    }
  };
}
