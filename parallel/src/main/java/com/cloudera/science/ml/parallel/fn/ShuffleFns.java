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
package com.cloudera.science.ml.parallel.fn;

import java.util.Random;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

public final class ShuffleFns {

  public static <T> DoFn<T, Pair<Integer, Pair<Integer, T>>> get(int numSplits, Long seed) {
    return new ShuffleFn<T>(seed, numSplits);
  }
  
  public static <T> DoFn<Pair<Integer, T>, Pair<Pair<Integer, Integer>, Pair<Integer, T>>> getForCrossValidated(
      int numSplits, Long seed, int numFolds) {
    return new CVShuffleFn<T>(numSplits, seed, numFolds);
  }
  
  private static class ShuffleFn<T> extends MapFn<T, Pair<Integer, Pair<Integer, T>>> {
    private final Long seed;
    private final int numSplits;
    
    private transient Random rand;
    
    public ShuffleFn(Long seed, int numSplits) {
      this.seed = seed;
      this.numSplits = numSplits;
    }
    
    @Override
    public void initialize() {
     this.rand = seed == null ? new Random() : new Random(seed);
    }
    
    @Override
    public Pair<Integer, Pair<Integer, T>> map(T input) {
      return Pair.of(rand.nextInt(numSplits), Pair.of(rand.nextInt(), input));
    }
  }
  
  private static class CVShuffleFn<T> extends
      DoFn<Pair<Integer, T>, Pair<Pair<Integer, Integer>, Pair<Integer, T>>> {

    private final int numFolds;
    private final int numSplits;
    private final Long seed;

    private transient Random random;

    public CVShuffleFn(int numSplits, Long seed, int numFolds) {
      this.numSplits = numSplits;
      this.seed = seed;
      this.numFolds = numFolds;
    }

    @Override
    public float scaleFactor() {
      return numFolds - 1.0f;
    }

    @Override
    public void initialize() {
      this.random = (seed == null) ? new Random() : new Random(seed);
    }

    @Override
    public void process(Pair<Integer, T> tagged,
        Emitter<Pair<Pair<Integer, Integer>, Pair<Integer, T>>> emitter) {
      int fold = tagged.first();
      for (int i = 0; i < numFolds; i++) {
        if (i != fold) {
          Pair<Integer, Integer> key = Pair.of(i, random.nextInt(numSplits));
          Pair<Integer, T> value = Pair.of(random.nextInt(), tagged.second());
          emitter.emit(Pair.of(key, value));
        }
      }
    }
  }

  private ShuffleFns() { }
}
