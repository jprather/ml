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
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.RecordSpec;
import com.cloudera.science.ml.core.records.SimpleRecord;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.parallel.summary.Summary;
import com.cloudera.science.ml.parallel.summary.SummaryStats;
import com.cloudera.science.ml.parallel.types.MLRecords;
import com.google.common.collect.Maps;

/**
 *
 */
public class Pivot {

  public static enum Agg { SUM, MEAN };
  
  private Summary summary;
  private Spec spec;
  
  public Pivot(Summary summary) {
    this.summary = summary;
    this.spec = summary.getSpec();
  }
  
  private Spec createSpec(List<Integer> groupColumns) {
    RecordSpec.Builder b = RecordSpec.builder();
    for (Integer c : groupColumns) {
      FieldSpec f = spec.getField(c);
      b.add(f.name(), f.spec());
    }
    return b.build();
  }
  
  public PCollection<Record> pivot(PCollection<Record> records,
      List<Integer> groupColumns,
      int attributeColumn,
      int valueColumn,
      Agg agg) {
    Spec keySpec = createSpec(groupColumns);
    PTableType<Record, Map<String, Stat>> ptt = Avros.tableOf(
        MLRecords.record(keySpec),
        Avros.maps(Avros.reflects(Stat.class)));

    RecordSpec.Builder b = RecordSpec.builder(keySpec);
    SummaryStats attrStats = summary.getStats(attributeColumn);
    SummaryStats valueStats = summary.getStats(valueColumn);
    List<String> levels = null;
    if (!valueStats.isNumeric()) {
      throw new IllegalArgumentException("Non-numeric value column in pivot op");
    } else if (attrStats.isNumeric() || attrStats.numLevels() == 1) {
      throw new IllegalArgumentException("Non-categorical attribute column in pivot op");
    } else {
      levels = attrStats.getLevels();
      for (String level : levels) {
        b.addDouble(level);
      }
    }
    
    Spec outSpec = b.build();
    return records.parallelDo("pivotmap",
        new PivotMapperFn(keySpec, groupColumns, attributeColumn, valueColumn),
        ptt)
        .groupByKey()
        .combineValues(new MapAggregator())
        .parallelDo("makerecord",
            new PivotFinishFn(outSpec, levels, agg),
            MLRecords.record(outSpec));
  }
  
  private static class PivotMapperFn extends DoFn<Record, Pair<Record, Map<String, Stat>>> {

    private final Spec spec;
    private final List<Integer> groupColumns;
    private final int attributeColumn;
    private final int valueColumn;
    private final Map<Record, Map<String, Stat>> cache;
    private int cacheAdds = 0;
    
    public PivotMapperFn(Spec spec, List<Integer> groupColumns, int attributeColumn, int valueColumn) {
      this.spec = spec;
      this.groupColumns = groupColumns;
      this.attributeColumn = attributeColumn;
      this.valueColumn = valueColumn;
      this.cache = Maps.newHashMap();
    }
    
    @Override
    public void process(Record r, Emitter<Pair<Record, Map<String, Stat>>> emitter) {
      Record key = new SimpleRecord(spec);
      for (int i = 0; i < groupColumns.size(); i++) {
        key.set(i, r.get(groupColumns.get(i)));
      }

      Map<String, Stat> ss = cache.get(key);
      if (ss == null) {
        ss = Maps.newHashMap();
        cache.put(key, ss);
      }
      
      String level = r.getAsString(attributeColumn);
      Stat stats = ss.get(level);
      if (stats == null) {
        stats = new Stat();
        ss.put(level, stats);
        cacheAdds++;
      }
      
      stats.inc(r.getAsDouble(valueColumn));
      
      if (cacheAdds > 10000) { //TODO parameterize
        cleanup(emitter);
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<Record, Map<String, Stat>>> emitter) {
      for (Map.Entry<Record, Map<String, Stat>> e : cache.entrySet()) {
        emitter.emit(Pair.of(e.getKey(), e.getValue()));
      }
      cache.clear();
      cacheAdds = 0;
    }
  }
  
  private static class PivotFinishFn extends MapFn<Pair<Record, Map<String, Stat>>, Record> {
    private Spec spec;
    private List<String> levels;
    private Agg agg;
    
    public PivotFinishFn(Spec spec, List<String> levels, Agg agg) {
      this.spec = spec;
      this.levels = levels;
      this.agg = agg;
    }
    
    @Override
    public Record map(Pair<Record, Map<String, Stat>> p) {
      Record r = new SimpleRecord(spec);
      int index = p.first().getSpec().size();
      for (int i = 0; i < index; i++) {
        r.set(i, p.first().get(i));
      }
      for (int i = 0; i < levels.size(); i++) {
        Stat ss = p.second().get(levels.get(i));
        double stat = 0.0;
        if (ss != null) {
          if (agg == Agg.MEAN) {
            stat = ss.sum / ss.count;
          } else {
            stat = ss.sum;
          }
        }
        r.set(index + i, stat);
      }
      return r;
    }
  }

}
