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

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.cloudera.science.ml.avro.MLWeightedCenters;
import com.cloudera.science.ml.client.params.InputParameters;
import com.cloudera.science.ml.client.params.PipelineParameters;
import com.cloudera.science.ml.client.params.RandomParameters;
import com.cloudera.science.ml.client.util.AvroIO;
import com.cloudera.science.ml.core.vectors.VectorConvert;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.cloudera.science.ml.kmeans.parallel.KMeansParallel;
import com.cloudera.science.ml.parallel.crossfold.Crossfold;
import com.google.common.collect.Lists;

@Parameters(commandDescription = "Creates a sketch of a large dataset via the k-means|| (scalable k-means++) algorithm")
public class KMeansSketchCommand implements Command {

  @Parameter(names = "--init-vectors-path",
      description = "The file that contains the vector(s) used for initializing k-means||")
  private String initVectorsPath;

  @Parameter(names = "--cross-folds",
      description = "The number of sketches to create via cross-")
  private int crossFolds = 2;
  
  @Parameter(names = "--iterations",
      description = "The number of iterations of k-means|| to run")
  private int numIterations = 5;
  
  @Parameter(names = "--points-per-iteration", required=true,
      description = "The The number of sample points to select on each iteration")
  private int samplesPerIteration;

  @Parameter(names = "--output-file", required=true,
      description = "A local file to write the sketch output to (as Avro MLWeightedCenters records)")
  private String outputFile;
  
  @ParametersDelegate
  private PipelineParameters pipelineParams = new PipelineParameters();
  
  @ParametersDelegate
  private InputParameters inputParams = new InputParameters();
  
  @ParametersDelegate
  private RandomParameters randomParams = new RandomParameters();
  
  @Override
  public int execute(Configuration conf) throws Exception {
    Pipeline p = pipelineParams.create(KMeansSketchCommand.class, conf);
    PCollection<Vector> input = inputParams.getVectors(p);
    List<Vector> initial = getInitialVectors(p);
    
    KMeansParallel kmp = new KMeansParallel(randomParams.getRandom());
    Crossfold cf = new Crossfold(crossFolds);
    
    List<List<Weighted<Vector>>> wv = kmp.initialization(input,
        numIterations, samplesPerIteration, initial, cf);
    AvroIO.write(toWeightedCenters(wv), new File(outputFile));
    return 0;
  }

  private List<MLWeightedCenters> toWeightedCenters(List<List<Weighted<Vector>>> in) {
    List<MLWeightedCenters> out = Lists.newArrayList();
    for (List<Weighted<Vector>> e : in) {
      MLWeightedCenters mlwc = MLWeightedCenters.newBuilder()
          .setCenters(Lists.transform(e, VectorConvert.FROM_WEIGHTED_VEC))
          .build();
      out.add(mlwc);
    }
    return out;
  }
  
  private List<Vector> getInitialVectors(Pipeline p) {
    if (initVectorsPath != null) {
      PCollection<Vector> init = inputParams.getVectorsFromPath(p,
          initVectorsPath);
      return Lists.newArrayList(init.materialize());
    } else {
      throw new IllegalArgumentException("No initial vector config specified");
    }
  }
  
  @Override
  public String getDescription() {
    return "Creates a sketch of a large dataset via the k-means|| (scalable k-means++) algorithm";
  }

}
