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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.materialize.pobject.PObjectImpl;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 */
public class Summarizer {
  
  private Set<Integer> ignoredColumns = Sets.newHashSet();
  private boolean defaultToSymbolic = false;
  private Set<Integer> exceptionColumns = Sets.newHashSet();
  private Spec spec = null;
  
  public Summarizer() { }
  
  public Summarizer spec(Spec spec) {
    this.spec = spec;
    return this;
  }
  
  public Summarizer ignoreColumns(Integer... columns) {
    return ignoreColumns(Arrays.asList(columns));
  }

  public Summarizer defaultToSymbolic(boolean defaultToSymbolic) {
    this.defaultToSymbolic = defaultToSymbolic;
    return this;
  }

  public Summarizer ignoreColumns(Iterable<Integer> columns) {
    for (Integer c : columns) {
      ignoredColumns.add(c);
    }
    return this;
  }

  public Summarizer exceptionColumns(Integer... columns) {
    return exceptionColumns(Arrays.asList(columns));
  }

  public Summarizer exceptionColumns(Iterable<Integer> columns) {
    for (Integer c : columns) {
      exceptionColumns.add(c);
    }
    return this;
  }

  public PObject<Summary> build(PCollection<Record> input) {
    return new SummaryPObject(spec, input.parallelDo("summarize",
        new SummarizeFn(ignoredColumns, defaultToSymbolic, exceptionColumns),
        Avros.tableOf(Avros.ints(), Avros.pairs(Avros.longs(), Avros.reflects(SummaryStats.class))))
        .groupByKey(1)
        .combineValues(Aggregators.pairAggregator(Aggregators.SUM_LONGS(), SummaryStats.AGGREGATOR)));
  }

  private static class SummaryPObject extends PObjectImpl<Pair<Integer, Pair<Long, SummaryStats>>, Summary> {
    private final Spec spec;
    
    public SummaryPObject(Spec spec, PCollection<Pair<Integer, Pair<Long, SummaryStats>>> pc) {
      super(pc);
      this.spec = spec;
    }
    
    @Override
    protected Summary process(Iterable<Pair<Integer, Pair<Long, SummaryStats>>> iter) {
      Map<Integer, SummaryStats> ss = Maps.newHashMap();
      int fieldCount = 0;
      long recordCount = 0L;
      for (Pair<Integer, Pair<Long, SummaryStats>> p : iter) {
        fieldCount++;
        recordCount = p.second().first();
        ss.put(p.first(), p.second().second());
      }
      return new Summary(spec, recordCount, fieldCount, ss);
    }
  }

  private static class SummarizeFn extends DoFn<Record, Pair<Integer, Pair<Long, SummaryStats>>> {
    private final Set<Integer> ignoredColumns;
    private final boolean defaultToSymbolic;
    private final Set<Integer> exceptionColumns;
    private final Map<Integer, SummaryStats> stats;
    private long count;
    
    public SummarizeFn(Set<Integer> ignoreColumns,
        boolean defaultToSymbolic, Set<Integer> exceptionColumns) {
      this.ignoredColumns = ignoreColumns;
      this.defaultToSymbolic = defaultToSymbolic;
      this.exceptionColumns = exceptionColumns;
      this.stats = Maps.newHashMap();
      this.count = 0;
    }
    
    @Override
    public void process(Record record,
        Emitter<Pair<Integer, Pair<Long, SummaryStats>>> emitter) {
      for (int idx = 0; idx < record.getSpec().size(); idx++) {
        if (!ignoredColumns.contains(idx)) {
          SummaryStats ss = stats.get(idx);
          if (ss == null) {
            ss = new SummaryStats();
            stats.put(idx, ss);
          }
          boolean symbolic = exceptionColumns.contains(idx) ? !defaultToSymbolic : defaultToSymbolic;
          if (symbolic) {
            ss.addSymbol(record.getAsString(idx));
          } else {
            ss.addNumeric(record.getAsDouble(idx));
          }
        }
      }
      count++;
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<Long, SummaryStats>>> emitter) {
      for (Map.Entry<Integer, SummaryStats> e : stats.entrySet()) {
        emitter.emit(Pair.of(e.getKey(), Pair.of(count, e.getValue())));
      }
      stats.clear();
    }
  }
}
