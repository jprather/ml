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

import java.util.regex.Pattern;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.csv.CSVRecord;
import com.cloudera.science.ml.parallel.types.MLRecords;

/**
 *
 */
public class StringSplitFn extends DoFn<String, Record> {

  private final String delim;
  private final Pattern ignoredLines;
  
  public static PCollection<Record> apply(PCollection<String> in, String delim) {
    return apply(in, delim, null);
  }
  
  public static PCollection<Record> apply(PCollection<String> in, String delim,
      Pattern ignoredLines) {
    return in.parallelDo("string-split",
        new StringSplitFn(delim, ignoredLines),
        MLRecords.csvRecord(in.getTypeFamily(), delim));
  }
  
  public StringSplitFn(String delim, Pattern ignoredLines) {
    this.delim = delim;
    this.ignoredLines = ignoredLines;
  }

  @Override
  public void process(String line, Emitter<Record> emitter) {
    if (line == null || line.isEmpty()) {
      return;
    } else if (ignoredLines != null && ignoredLines.matcher(line).find()) {
      return;
    }
    emitter.emit(new CSVRecord(line.split(delim)));
  }
}
