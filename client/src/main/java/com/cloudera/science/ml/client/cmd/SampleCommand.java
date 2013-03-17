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

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.lib.Sample;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.cloudera.science.ml.client.params.InputParameters;
import com.cloudera.science.ml.client.params.OutputParameters;
import com.cloudera.science.ml.client.params.PipelineParameters;
import com.cloudera.science.ml.parallel.sample.ReservoirSampling;

@Parameters(commandDescription = "Samples from a dataset and writes the sampled data to HDFS or a local file")
public class SampleCommand implements Command {

  @Parameter(names = "--output",
      description = "The name of a file to store the output samples in")
  private String sampleFile;
  
  @Parameter(names = "--reservoir-sample-size",
      description = "Samples N records uniformly from the input. Mutually exclusive with --sampling-prob")
  private int sampleSize = 0;
  
  @Parameter(names = "--sampling-prob",
      description = "Sample each record in the input independently with the given probability. Mutually exclusive with --reservoir-sample-size.")
  private double samplingProbability = 0.0;
  
  @ParametersDelegate
  private PipelineParameters pipelineParams = new PipelineParameters();
  
  @ParametersDelegate
  private InputParameters inputParams = new InputParameters();

  @ParametersDelegate
  private OutputParameters outputParams = new OutputParameters();
  
  @Override
  public int execute(Configuration conf) throws Exception {
    Pipeline p = pipelineParams.create(SampleCommand.class, conf);
    PCollection<?> elements = inputParams.getRaw(p);
    
    PCollection<?> sample = null;
    if (sampleSize > 0 && samplingProbability > 0.0) {
      throw new IllegalArgumentException("--reservoir-sampling-size and --sampling-prob are mutually exclusive options.");
    } else if (sampleSize > 0) {
      sample = ReservoirSampling.sample(elements, sampleSize);
    } else if (samplingProbability > 0.0 && samplingProbability < 1.0) {
      sample = Sample.sample(elements, samplingProbability);
    } else {
      throw new IllegalArgumentException(String.format(
          "Invalid input args: sample size = %d, sample prob = %.4f", 
          sampleSize, samplingProbability));
    }
    outputParams.write(sample, sampleFile);
    
    PipelineResult pr = p.done();
    return pr.succeeded() ? 0 : 1;
  }

  @Override
  public String getDescription() {
    return "Samples from a dataset and writes the sampled data to HDFS or a local file";
  }

}
