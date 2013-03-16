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
package com.cloudera.science.ml.parallel.pivot;

import java.util.List;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.parallel.summary.Summary;
import com.cloudera.science.ml.parallel.summary.SummaryStats;
import com.google.common.collect.Maps;

/**
 *
 */
public class Pivot {

  public static enum Agg { SUM, MEAN };
  
  private Summary summary;
  
  public Pivot(Summary summary) {
    this.summary = summary;
  }
  
  public PCollection<Record> pivot(PCollection<Record> records,
      List<String> groupColumns,
      int attributeColumn,
      int valueColumn,
      Agg agg) {
    return null;
    // Dynamically build up a schema-- probably.
  }
  
  private static class PivotMapperFn<T> extends DoFn<Record, Pair<T, Map<String, SummaryStats>>> {

    private final List<String> groupColumns;
    private final int attributeColumn;
    private final int valueColumn;
    private final Map<T, Map<String, SummaryStats>> cache;
    private int cacheAdds = 0;
    
    public PivotMapperFn(List<String> groupColumns, int attributeColumn, int valueColumn) {
      this.groupColumns = groupColumns;
      this.attributeColumn = attributeColumn;
      this.valueColumn = valueColumn;
      this.cache = Maps.newHashMap();
    }
    
    @Override
    public void process(Record r, Emitter<Pair<T, Map<String, SummaryStats>>> emitter) {
      T keyType = null; // TODO
      String level = r.getAsString(attributeColumn);
      double value = r.getAsDouble(valueColumn);
      
      Map<String, SummaryStats> ss = cache.get(keyType);
      if (ss == null) {
        ss = Maps.newHashMap();
        cache.put(keyType, ss);
      }
      
      SummaryStats stats = ss.get(level);
      if (stats == null) {
        stats = new SummaryStats();
        ss.put(level, stats);
        cacheAdds++;
      }
      
      stats.addNumeric(value);
      
      if (cacheAdds > 10000) { //TODO parameterize
        cleanup(emitter);
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<T, Map<String, SummaryStats>>> emitter) {
      for (Map.Entry<T, Map<String, SummaryStats>> e : cache.entrySet()) {
        emitter.emit(Pair.of(e.getKey(), e.getValue()));
      }
      cache.clear();
      cacheAdds = 0;
    }
  }
}
