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
import org.apache.crunch.io.To;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.cloudera.science.ml.avro.MLCenters;
import com.cloudera.science.ml.avro.MLClusterAssignment;
import com.cloudera.science.ml.client.params.InputParameters;
import com.cloudera.science.ml.client.params.PipelineParameters;
import com.cloudera.science.ml.client.params.RandomParameters;
import com.cloudera.science.ml.client.util.AvroIO;
import com.cloudera.science.ml.core.vectors.VectorConvert;
import com.cloudera.science.ml.kmeans.parallel.KMeansParallel;
import com.google.common.collect.Lists;

@Parameters(commandDescription =
    "Apply a set of centers to a dataset and output the resulting assignments/distances")
public class KMeansAssignmentCommand implements Command {

  @Parameter(names = "--centers-file", required=true,
      description = "The local Avro file containing the centers to be applied")
  private String centersFile;
  
  @Parameter(names = "--output-path", required=true,
      description = "The path to write the output assignments to on the cluster")
  private String assignmentsPath;
  
  @Parameter(names = "--output-type",
      description = "The format of the output assignments: Either 'avro' or 'csv'")
  private String assignmentsType = "avro";
  
  @ParametersDelegate
  private InputParameters inputParams = new InputParameters();
  
  @ParametersDelegate
  private PipelineParameters pipelineParams = new PipelineParameters();
  
  @ParametersDelegate
  private RandomParameters randomParams = new RandomParameters();
  
  @Override
  public int execute(Configuration conf) throws Exception {
    Pipeline p = pipelineParams.create(KMeansAssignmentCommand.class, conf);
    PCollection<Vector> input = inputParams.getVectors(p);
    List<MLCenters> centers = AvroIO.read(MLCenters.class, new File(centersFile));
    KMeansParallel kmp = new KMeansParallel(randomParams.getRandom());
    PCollection<MLClusterAssignment> assigned = kmp.computeClusterAssignments(input,
        Lists.transform(centers, VectorConvert.TO_CENTERS));
    if ("avro".equals(assignmentsType)) {
      p.write(assigned, To.avroFile(assignmentsPath));
    } else {
      //TODO: as text
    }
    p.done();
    return 0;
  }

  @Override
  public String getDescription() {
    return "Apply a set of centers to a dataset and output the resulting assignments/distances";
  }

}
