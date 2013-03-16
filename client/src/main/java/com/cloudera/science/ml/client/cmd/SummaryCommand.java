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
import java.util.Arrays;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.cloudera.science.ml.client.params.InputParameters;
import com.cloudera.science.ml.client.params.PipelineParameters;
import com.cloudera.science.ml.client.params.Specs;
import com.cloudera.science.ml.client.params.SummaryParameters;
import com.cloudera.science.ml.core.records.BasicSpec;
import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.parallel.summary.Summarizer;
import com.cloudera.science.ml.parallel.summary.Summary;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

@Parameters(commandDescription = "Summarize the continuous (and optionally, categorical) attributes of a CSV or Vector file")
public class SummaryCommand implements Command {

  @Parameter(names = "--column-header-file",
      description = "The name of a file whose first line is a CSV with the name of each of the input columns")
  private String headerFile;
  
  @Parameter(names = "--symbolic-columns",
      description = "A CSV of ints or strings that specifies columns that represent categorical variables (zero-indexed)",
      splitter = CommaParameterSplitter.class)
  private List<String> symbolicColumns = Lists.newArrayList();

  @Parameter(names = "--numeric-columns",
      description = "A CSV of ints or strings that specifies columns that represent numeric variables (zero-indexed)",
      splitter = CommaParameterSplitter.class)
  private List<String> numericColumns = Lists.newArrayList();

  @Parameter(names = "--ignore-columns",
      description = "A CSV of ints or strings that specifies columns in the input to ignore (zero-indexed)",
      splitter = CommaParameterSplitter.class)
  List<String> ignoredColumns = Lists.newArrayList();
  
  @Parameter(names = "--id-column",
      description = "For CSV inputs, the column of the file that contains the name/id of the record")
  private String idColumn = "-1";

  @Parameter(names = "--summary-file", required=true,
      description = "The name of the local file to store the JSON summary data to")
  private String summaryFile;

  @ParametersDelegate
  private InputParameters inputParams = new InputParameters();
  
  @ParametersDelegate
  private PipelineParameters pipelineParams = new PipelineParameters();
  
  @ParametersDelegate
  private SummaryParameters summaryParams = new SummaryParameters();
  
  @Override
  public int execute(Configuration conf) throws Exception {
    Pipeline p = pipelineParams.create(SummaryCommand.class, conf);
    PCollection<Record> records = inputParams.getRecords(p);

    Spec spec = null;
    if (headerFile != null) {
      String line = Files.readFirstLine(new File(headerFile), Charsets.UTF_8);
      spec = new BasicSpec(DataType.STRING,
          Arrays.asList(line.split(inputParams.getDelimiter())));
    }
    
    boolean defaultToSymbolic = false;
    List<Integer> exceptionColumns = null;
    if (!symbolicColumns.isEmpty()) {
      exceptionColumns = Specs.getFieldIds(spec, symbolicColumns);
    } else {
      defaultToSymbolic = true;
      exceptionColumns = Specs.getFieldIds(spec, numericColumns);
    }
    List<Integer> intIgnoredColumns = Specs.getFieldIds(spec, ignoredColumns);
    
    Summarizer summarizer = new Summarizer()
        .spec(spec)
        .defaultToSymbolic(defaultToSymbolic)
        .exceptionColumns(exceptionColumns)
        .ignoreColumns(intIgnoredColumns)
        .ignoreColumns(Specs.getFieldId(spec, idColumn));
    Summary summary = summarizer.build(records).getValue();
    summaryParams.save(summary, summaryFile);

    p.done();
    return 0;
  }

  @Override
  public String getDescription() {
    return "Summarize the continuous (and optionally, categorical) attributes of a CSV or Vector file";
  }

}
