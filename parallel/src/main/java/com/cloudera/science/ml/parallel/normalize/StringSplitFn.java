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

import java.util.List;
import java.util.regex.Pattern;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;

import com.google.common.collect.Lists;

/**
 *
 */
public class StringSplitFn extends DoFn<String, Elements> {

  private final String delim;
  private final Pattern ignoredLines;
  
  public static PCollection<Elements> apply(PCollection<String> in, String delim) {
    return apply(in, delim, null);
  }
  
  public static PCollection<Elements> apply(PCollection<String> in, String delim,
      Pattern ignoredLines) {
    return in.parallelDo("string-split",
        new StringSplitFn(delim, ignoredLines),
        ElementsList.PTYPE);
  }
  
  public StringSplitFn(String delim, Pattern ignoredLines) {
    this.delim = delim;
    this.ignoredLines = ignoredLines;
  }

  @Override
  public void process(String line, Emitter<Elements> emitter) {
    if (line == null || line.isEmpty()) {
      return;
    } else if (ignoredLines != null && ignoredLines.matcher(line).find()) {
      return;
    }
    List<Element> ret = Lists.newArrayList();
    int fieldId = 0;
    for (String s : line.split(delim)) {
      ret.add(new Element(fieldId, s));
      fieldId++;
    }
    emitter.emit(new ElementsList(ret));
  }
}
