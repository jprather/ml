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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.types.PType;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 */
public class Standardizer implements Serializable {

  private static final Log LOG = LogFactory.getLog(Standardizer.class);
  
  private final Summary summary;
  private final boolean sparse;
  private final int idColumn;
  private final Set<Integer> ignoredColumns;
  private final Transform defaultTransform;
  private final Map<Integer, Transform> transforms;
  private final int expansion;
  
  public static Builder builder() { 
    return new Builder();
  }
  
  public static class Builder {
    private Summary s = new Summary();
    private boolean sparse;
    private int idColumn = -1;
    private Set<Integer> ignoredColumns = Sets.newHashSet();
    private Transform defaultTransform = Transform.NONE;
    private Map<Integer, Transform> transforms = Maps.newHashMap();
    
    public Builder summary(Summary s) {
      this.s = s;
      return this;
    }
    
    public Builder sparse(boolean sparse) {
      this.sparse = sparse;
      return this;
    }
    
    public Builder idColumn(int idColumn) {
      this.idColumn = idColumn;
      return this;
    }
    
    public Builder defaultTransform(Transform t) {
      this.defaultTransform = t;
      return this;
    }
    
    public Builder addCustomTransform(int columnId, Transform t) {
      this.transforms.put(columnId, t);
      return this;
    }
    
    public Builder ignoredColumns(Integer... columnIds) {
      return ignoreColumns(Arrays.asList(columnIds));
    }
    
    public Builder ignoreColumns(Collection<Integer> ids) {
      this.ignoredColumns.addAll(ids);
      return this;
    }
    
    public Standardizer build() {
      return new Standardizer(s, sparse, idColumn, ignoredColumns, defaultTransform,
          transforms);
    }
  }
  
  private Standardizer(Summary summary, boolean sparse, int idColumn,
      Set<Integer> ignoredColumns, Transform defaultTransform,
      Map<Integer, Transform> transforms) {
    this.summary = summary;
    this.sparse = sparse;
    this.idColumn = idColumn;
    this.ignoredColumns = ignoredColumns;
    this.defaultTransform = defaultTransform;
    this.transforms = transforms;
    this.expansion = -ignoredColumns.size() + (idColumn >= 0 ? -1 :  0) + summary.getNetLevels();
  }
  
  public <V extends Vector> PCollection<V> apply(PCollection<Elements> elems, PType<V> ptype) {
    return elems.parallelDo("standardize", new StandardizeFn<V>(), ptype);
  }
  
  private class StandardizeFn<V extends Vector> extends MapFn<Elements, V> {
    @Override
    public V map(Elements elements) {
      double[] values = new double[elements.size() + expansion];
      int offset = 0;
      for (int i = 0; i < elements.size(); i++) {
        if (idColumn != i && !ignoredColumns.contains(i)) {
          Element e = elements.get(i);
          SummaryStats ss = summary.getStats(i);
          if (ss == null) {
            values[offset] = e.getNumeric();
            offset++;
          } else if (ss.isNumeric()) {
            Transform t = defaultTransform;
            if (transforms.containsKey(i)) {
              t = transforms.get(i);
            }
            values[offset] = t.apply(e.getNumeric(), summary.getRecordCount(), ss);
            offset++;
          } else {
            int index = ss.index(e.getSymbolic());
            if (index < 0) {
              LOG.error(String.format("Unknown value encountered for field %d: '%s'",
                  i, e.getSymbolic()));
            } else {
              values[offset + index] = 1.0;
            }
            offset += ss.numLevels();
          }
        }
      }
      Vector v = elements.asVector(values, sparse);
      if (idColumn >= 0) {
        v = new NamedVector(v, elements.get(idColumn).getSymbolic());
      }
      return (V) v;
    }
  }
}
