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
package com.cloudera.science.ml.parallel.sample;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Generates a weighted random sample of N items from a distributed data set using the reservoir
 * algorithm described in <a href="http://utopia.duth.gr/~pefraimi/research/data/2007EncOfAlg.pdf">Efraimidis
 * and Spirakis (2005)</a>.
 * 
 */
public class ReservoirSampling {
  
  public static <T> PCollection<T> sample(
      PCollection<T> input,
      int sampleSize) {
    return sample(input, sampleSize, null);
  }

  public static <T> PCollection<T> sample(
      PCollection<T> input,
      int sampleSize,
      Random random) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<Pair<T, Integer>> ptype = ptf.pairs(input.getPType(), ptf.ints());
    return weightedSample(
        input.parallelDo(new MapFn<T, Pair<T, Integer>>() {
          public Pair<T, Integer> map(T t) { return Pair.of(t, 1); }
        }, ptype),
        sampleSize,
        random);
  }
  
  public static <T, N extends Number> PCollection<T> weightedSample(
      PCollection<Pair<T, N>> input,
      int sampleSize) {
    return weightedSample(input, sampleSize, null);
  }
  
  public static <T, N extends Number> PCollection<T> weightedSample(
      PCollection<Pair<T, N>> input,
      int sampleSize,
      Random random) {
    PTypeFamily ptf = input.getTypeFamily();
    PTable<Integer, Pair<T, N>> groupedIn = input.parallelDo(
        new MapFn<Pair<T, N>, Pair<Integer, Pair<T, N>>>() {
          @Override
          public Pair<Integer, Pair<T, N>> map(Pair<T, N> p) {
            return Pair.of(0, p);
          }
        }, ptf.tableOf(ptf.ints(), input.getPType()));
    int[] ss = new int[] { sampleSize };
    return groupedWeightedSample(groupedIn, ss, random)
        .parallelDo(new MapFn<Pair<Integer, T>, T>() {
          @Override
          public T map(Pair<Integer, T> p) {
            return p.second();
          }
        }, (PType<T>) input.getPType().getSubTypes().get(0));
  }
  
  public static <T, N extends Number> PCollection<Pair<Integer, T>> groupedWeightedSample(
      PTable<Integer, Pair<T, N>> input,
      int[] sampleSizes,
      Random random) {
    PTypeFamily ptf = input.getTypeFamily();
    PType<T> ttype = (PType<T>) input.getPTableType().getValueType().getSubTypes().get(0);
    PTableType<Integer, Pair<Double, T>> ptt = ptf.tableOf(ptf.ints(),
        ptf.pairs(ptf.doubles(), ttype));
    
    return input.parallelDo(new SampleFn<T, N>(sampleSizes, random), ptt)
        .groupByKey(1)
        .combineValues(new WRSCombineFn<T>(sampleSizes))
        .parallelDo(new MapFn<Pair<Integer, Pair<Double, T>>, Pair<Integer, T>>() {
          @Override
          public Pair<Integer, T> map(Pair<Integer, Pair<Double, T>> p) {
            return Pair.of(p.first(), p.second().second());
          }
        }, ptf.pairs(ptf.ints(), ttype));
  }
  
  private static class SampleFn<T, N extends Number>
      extends DoFn<Pair<Integer, Pair<T, N>>, Pair<Integer, Pair<Double, T>>> {
  
    private int[] sampleSizes;
    private transient List<SortedMap<Double, T>> reservoirs;
    private Random random;
    
    public SampleFn(int[] sampleSizes, Random random) {
      this.sampleSizes = sampleSizes;
      this.random = random;
    }
    
    @Override
    public void initialize() {
      this.reservoirs = Lists.newArrayList();
      for (int i = 0; i < sampleSizes.length; i++) {
        reservoirs.add(Maps.<Double, T>newTreeMap());
      }
      if (random == null) {
        this.random = new Random();
      }
    }
    
    @Override
    public void process(Pair<Integer, Pair<T, N>> input,
        Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      int id = input.first();
      Pair<T, N> p = input.second();
      double weight = p.second().doubleValue();
      if (weight > 0.0) {
        double score = Math.log(random.nextDouble()) / weight;
        SortedMap<Double, T> reservoir = reservoirs.get(id);
        if (reservoir.size() < sampleSizes[id]) { 
          reservoir.put(score, p.first());        
        } else if (score > reservoir.firstKey()) {
          reservoir.remove(reservoir.firstKey());
          reservoir.put(score, p.first());
        }
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      for (int id = 0; id < reservoirs.size(); id++) {
        SortedMap<Double, T> reservoir = reservoirs.get(id);
        for (Map.Entry<Double, T> e : reservoir.entrySet()) {
          emitter.emit(Pair.of(id, Pair.of(e.getKey(), e.getValue())));
        }
      }
    }
  }
  
  private static class WRSCombineFn<T> extends CombineFn<Integer, Pair<Double, T>> {

    private int[] sampleSizes;
    private List<SortedMap<Double, T>> reservoirs;
    
    public WRSCombineFn(int[] sampleSizes) {
      this.sampleSizes = sampleSizes;
    }

    @Override
    public void initialize() {
      this.reservoirs = Lists.newArrayList();
      for (int i = 0; i < sampleSizes.length; i++) {
        reservoirs.add(Maps.<Double, T>newTreeMap());
      }
    }
    
    @Override
    public void process(Pair<Integer, Iterable<Pair<Double, T>>> input,
        Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      SortedMap<Double, T> reservoir = reservoirs.get(input.first());
      for (Pair<Double, T> p : input.second()) {
        if (reservoir.size() < sampleSizes[input.first()]) { 
          reservoir.put(p.first(), p.second());        
        } else if (p.first() > reservoir.firstKey()) {
          reservoir.remove(reservoir.firstKey());
          reservoir.put(p.first(), p.second());  
        }
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      for (int i = 0; i < reservoirs.size(); i++) {
        SortedMap<Double, T> reservoir = reservoirs.get(i);
        for (Map.Entry<Double, T> e : reservoir.entrySet()) {
          emitter.emit(Pair.of(i, Pair.of(e.getKey(), e.getValue())));
        }
      }
    }
  }
}
