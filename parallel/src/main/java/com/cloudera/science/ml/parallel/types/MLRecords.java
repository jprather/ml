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
package com.cloudera.science.ml.parallel.types;

import java.util.List;

import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.csv.CSVRecord;
import com.cloudera.science.ml.core.records.vectors.VectorRecord;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 *
 */
public class MLRecords {
  
  public static PType<Record> csvRecord(PTypeFamily ptf, String delim) {
    return ptf.derived(Record.class,
        new CSV2RecordMapFn(delim),
        new Record2CSVMapFn(delim),
        ptf.strings());
  }
  
  public static <V extends Vector> PType<Record> vectorRecord(PType<V> ptype) {
    return ptype.getFamily().derived(Record.class,
        new MapFn<V, Record>() {
          @Override
          public Record map(V v) {
            return new VectorRecord(v);
          }
        },
        new MapFn<Record, V>() {
          @Override
          public V map(Record r) {
            return (V) ((VectorRecord) r).getVector();
          }   
        }, ptype);
  }
  
  private static class CSV2RecordMapFn extends MapFn<String, Record> {

    private final String delim;
    
    public CSV2RecordMapFn(String delim) {
      this.delim = delim;
    }
    
    @Override
    public Record map(String str) {
      return new CSVRecord(str.split(delim));
    }
  };
  
  private static class Record2CSVMapFn extends MapFn<Record, String> {
    private final String delim;
    
    public Record2CSVMapFn(String delim) {
      this.delim = delim;
    }
    
    @Override
    public String map(Record r) {
      List<String> entries = Lists.newArrayList();
      for (int i = 0; i < r.getSpec().size(); i++) {
        entries.add(r.getAsString(i));
      }
      return Joiner.on(delim).join(entries);
    }
  }
}
