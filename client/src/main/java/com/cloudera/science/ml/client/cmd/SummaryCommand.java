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
package com.cloudera.science.ml.client.cmd;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.cloudera.science.ml.client.params.InputParameters;
import com.cloudera.science.ml.client.params.PipelineParameters;
import com.cloudera.science.ml.client.params.SummaryParameters;
import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.RecordSpec;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.parallel.summary.Summarizer;
import com.cloudera.science.ml.parallel.summary.Summary;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

@Parameters(commandDescription = "Summarize the continuous (and optionally, categorical) attributes of a CSV or Vector file")
public class SummaryCommand implements Command {

  @Parameter(names = "--header-file",
      description = "Local file that has info about each column in the input, one column per-line")
  private String headerFile;
  
  @Parameter(names = "--summary-file", required=true,
      description = "The name of the local file to store the JSON summary data to")
  private String summaryFile;

  @ParametersDelegate
  private InputParameters inputParams = new InputParameters();
  
  @ParametersDelegate
  private PipelineParameters pipelineParams = new PipelineParameters();
  
  @ParametersDelegate
  private SummaryParameters summaryParams = new SummaryParameters();
  
  // Indicators that a header file contains metadata
  private static final Set<String> SYMBOL_META = ImmutableSet.of("symbolic",
      "categorical", "nominal", "string");
  private static final Set<String> NUMERIC_META = ImmutableSet.of("numeric",
      "continuous", "real", "double");
  
  @Override
  public int execute(Configuration conf) throws Exception {
    Pipeline p = pipelineParams.create(SummaryCommand.class, conf);
    PCollection<Record> records = inputParams.getRecords(p);

    Spec spec = null;
    List<Integer> symbolicColumns = Lists.newArrayList();
    List<Integer> ignoredColumns = Lists.newArrayList();
    if (headerFile != null) {
      List<String> lines = Files.readLines(new File(headerFile), Charsets.UTF_8);
      RecordSpec.Builder rsb = RecordSpec.builder();
      for (int i = 0; i < lines.size(); i++) {
        String line = lines.get(i);
        if (line.contains(",")) {
          String[] pieces = line.split(",");
          if (pieces.length != 2) {
            throw new CommandException("Invalid header file row: " + line);
          }
          String name = pieces[0];
          String meta = pieces[1].toLowerCase().trim();
          if (meta.startsWith("ignore") || meta.startsWith("id")) {
            ignoredColumns.add(i);
            rsb.add(name, DataType.STRING);
          } else if (SYMBOL_META.contains(meta)) {
            symbolicColumns.add(i);
            rsb.add(name, DataType.STRING);
          } else if (NUMERIC_META.contains(meta)) {
            rsb.add(name, DataType.DOUBLE);
          } else {
            throw new CommandException(String.format(
                "Did not recognize metadata %s for field %s", meta, name));
          }
        } else {
          rsb.add(line, DataType.DOUBLE);
        }
      }
      spec = rsb.build();
    }
    
    Summarizer summarizer = new Summarizer()
        .spec(spec)
        .defaultToSymbolic(false)
        .exceptionColumns(symbolicColumns)
        .ignoreColumns(ignoredColumns);
    Summary summary = summarizer.build(records).getValue();
    summaryParams.save(summary, summaryFile);

    p.done();
    return 0;
  }

  @Override
  public String getDescription() {
    return "Summarize the continuous and categorical attributes of a CSV or Vector file";
  }

}
