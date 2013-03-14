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
package com.cloudera.science.ml.parallel.crossfold;

import java.io.Serializable;
import java.util.Random;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.base.Preconditions;

/**
 * Supports creating partitions of {@code PCollection}s for performing
 * cross-validations.
 */
public class Crossfold implements Serializable {
  private int numFolds;
  private long seed;
  
  public Crossfold(int numFolds) {
    this(numFolds, System.currentTimeMillis());
  }
  
  public Crossfold(int numFolds, long seed) {
    Preconditions.checkArgument(numFolds > 0, "Number of folds must be greater than zero");
    this.numFolds = numFolds;
    this.seed = seed;
  }
  
  public int getNumFolds() {
    return numFolds;
  }

  public <T> PCollection<Pair<Integer, T>> apply(PCollection<T> pcollect) {
    PTypeFamily ptf = pcollect.getTypeFamily();
    PType<Pair<Integer, T>> pt = ptf.pairs(ptf.ints(), pcollect.getPType());
    return pcollect.parallelDo("apply", new MapFn<T, Pair<Integer, T>>() {
      private transient Random rand;
      
      @Override
      public void initialize() {
        if (rand == null) {
          this.rand = new Random(seed);
        }
      }
      
      @Override
      public Pair<Integer, T> map(T t) {
        return Pair.of(rand.nextInt(numFolds), t);
      }
      
    }, pt);
  }
}
