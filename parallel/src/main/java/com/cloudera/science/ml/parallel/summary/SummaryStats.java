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
package com.cloudera.science.ml.parallel.summary;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.Aggregator;
import org.apache.crunch.fn.Aggregators.SimpleAggregator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class SummaryStats implements Serializable {
  
  private static class Entry implements Serializable {
    public int id;
    public long count;
    
    public Entry() { }
    
    public Entry(int id) {
      this.id = id;
      this.count = 0;
    }
    
    public Entry inc() {
      return inc(1L);
    }
    
    public Entry inc(long count) {
      this.count += count;
      return this;
    }
  }
  
  private static class Numeric implements Serializable {
    private double min;
    private double max;
    private double sum;
    private double sumSq;
    
    public double mean(long count) {
      return sum / count;
    }
    
    public double stdDev(long count) {
      double m = mean(count);
      return Math.sqrt(sumSq / count - m * m);
    }
    
    public double range() {
      return min - max;
    }
    
    public double min() {
      return min;
    }
    
    public double max() {
      return max;
    }
    
    public void update(double d) {
      sum += d;
      sumSq += d * d;
      if (d < min) {
        min = d;
      }
      if (d > max) {
        max = d;
      }
    }
    
    public void merge(Numeric other) {
      sum += other.sum;
      sumSq += other.sumSq;
      if (other.min < min) {
        min = other.min;
      }
      if (other.max > max) {
        max = other.max;
      }
    }
  }
  
  private Numeric numeric;
  private Map<String, Entry> histogram;
  private String name;
  
  public static final Aggregator<SummaryStats> AGGREGATOR = new SimpleAggregator<SummaryStats>() {
    private SummaryStats agg = new SummaryStats();
    @Override
    public void reset() {
      agg = new SummaryStats();
    }

    @Override
    public Iterable<SummaryStats> results() {
      return ImmutableList.of(agg);
    }

    @Override
    public void update(SummaryStats other) {
      agg.merge(other);
    }
  };
  
  public static SummaryStats create(List<String> values) {
    SummaryStats ss = new SummaryStats();
    for (int i = 0; i < values.size(); i++) {
      ss.histogram.put(values.get(i), new Entry(i).inc());
    }
    return ss;
  }
  
  public SummaryStats() {
    this.numeric = null;
    this.histogram = null;
  }
  
  private Numeric numeric() {
    if (numeric == null) {
      numeric = new Numeric();
    }
    return numeric;
  }
  
  private Map<String, Entry> histogram() {
    if (histogram == null) {
      histogram = Maps.newHashMap();
    }
    return histogram;
  }
  
  public String getFieldName() {
    return name;
  }

  public void setFieldName(String fieldName) {
    this.name = fieldName;
  }

  public boolean isNumeric() {
    return numeric != null;
  }
  
  public double mean(long count) {
    return numeric().mean(count);
  }
  
  public double stdDev(long count) {
    return numeric().stdDev(count);
  }
  
  public double range() {
    return numeric().range();
  }
  
  public double min() {
    return numeric().min();
  }
  
  public double max() {
    return numeric().max();
  }
  
  public List<String> getLevels() {
    List<String> levels = Lists.newArrayList(histogram.keySet());
    Collections.sort(levels);
    return levels;
  }
  
  public int numLevels() {
    return histogram == null ? 1 : histogram.size();
  }
  
  public int index(String value) {
    Entry e = histogram().get(value);
    if (e == null) {
      return -1;
    } else {
      return e.id;
    }
  }
  
  public void addSymbol(String symbol) {
    Map<String, Entry> h = histogram();
    Entry entry = h.get(symbol);
    if (entry == null) {
      entry = new Entry(h.size()).inc(); // init with count = 1
      h.put(symbol, entry);
    } else {
      entry.inc();
    }
  }
  
  public void addNumeric(double value) {
    numeric().update(value);
  }
  
  public void merge(SummaryStats summaryStats) {
    if (summaryStats.numeric != null) {
      numeric().merge(summaryStats.numeric);
    } else {
      Map<String, Entry> h = histogram();
      Map<String, Entry> merged = Maps.newHashMap();
      Set<String> keys = Sets.newTreeSet(
          Sets.union(h.keySet(), summaryStats.histogram.keySet()));
      for (String key : keys) {
        Entry e = h.get(key);
        Entry entry = summaryStats.histogram.get(key);
        Entry newEntry = new Entry(merged.size());
        if (e != null) {
          newEntry.inc(e.count);
        }
        if (entry != null) {
          newEntry.inc(entry.count);
        }
        merged.put(key, newEntry);
      }
      this.histogram = merged;
    }
  }
}