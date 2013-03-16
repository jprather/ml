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
package com.cloudera.science.ml.parallel.normalize;

import static org.junit.Assert.assertEquals;

import java.util.regex.Pattern;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.cloudera.science.ml.parallel.summary.Summarizer;
import com.cloudera.science.ml.parallel.summary.Summary;
import com.cloudera.science.ml.parallel.types.MLAvros;
import com.google.common.collect.ImmutableList;

/**
 *
 */
public class StringParsingTest {

  @Test
  public void testSimple() {
    PCollection<String> input = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "1.0,2.0,3.0",
        "0.4,2.0,1.0",
        "3.2,17.0,29.0");
    PCollection<Record> elems = StringSplitFn.apply(input, ",");
    Normalizer s = Normalizer.builder().build();
    PCollection<Vector> vecs = s.apply(elems, MLAvros.vector());
    assertEquals(ImmutableList.of(Vectors.of(1, 2, 3), Vectors.of(0.4, 2, 1),
        Vectors.of(3.2, 17, 29)), vecs.materialize());
  }
  
  @Test
  public void testRegex() {
    PCollection<String> input = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "#A line of text we want to ignore",
        "1.0,2.0,3.0",
        "0.4,2.0,1.0",
        "#Another inline comment, just to be irritating",
        "3.2,17.0,29.0");
    PCollection<Record> elems = StringSplitFn.apply(input, ",",
        Pattern.compile("^#"));
    Normalizer s = Normalizer.builder().build();
    PCollection<Vector> vecs = s.apply(elems, MLAvros.vector());
    assertEquals(ImmutableList.of(Vectors.of(1, 2, 3), Vectors.of(0.4, 2, 1),
        Vectors.of(3.2, 17, 29)), vecs.materialize());
  }
  
  @Test
  public void testCategorical() {
    PCollection<String> input = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "1.0,a,3.0,y",
        "0.4,b,1.0,x",
        "3.2,c,29.0,z");
    PCollection<Record> elems = StringSplitFn.apply(input, ",");
    Summary s = new Summarizer()
      .defaultToSymbolic(true)
      .exceptionColumns(0, 2)
      .build(elems).getValue();
    Normalizer st = Normalizer.builder().summary(s).build();
    PCollection<Vector> vecs = st.apply(elems, MLAvros.vector());
    assertEquals(ImmutableList.of(
        Vectors.of(1.0, 1, 0, 0, 3.0, 0.0, 1.0, 0.0),
        Vectors.of(0.4, 0, 1, 0, 1.0, 1.0, 0.0, 0.0),
        Vectors.of(3.2, 0, 0, 1, 29.0, 0, 0, 1)),
        vecs.materialize());
  }
  
  @Test
  public void testNamed() {
    PCollection<String> input = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "1.0,a,3.0,y",
        "0.4,b,1.0,x",
        "3.2,c,29.0,z");
    PCollection<Record> elems = StringSplitFn.apply(input, ",");
    Summary s = new Summarizer()
      .defaultToSymbolic(false)
      .exceptionColumns(3)
      .ignoreColumns(1)
      .build(elems).getValue();
    Normalizer st = Normalizer.builder().summary(s).idColumn(1).build();
    PCollection<Vector> vecs = st.apply(elems, MLAvros.vector());
    assertEquals(ImmutableList.of(
        Vectors.named("a", 1.0, 3.0, 0.0, 1.0, 0.0),
        Vectors.named("b", 0.4, 1.0, 1.0, 0.0, 0.0),
        Vectors.named("c", 3.2, 29.0, 0.0, 0.0, 1.0)),
        vecs.materialize());
  }
}
