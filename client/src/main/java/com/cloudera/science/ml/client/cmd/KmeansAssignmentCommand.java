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
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.beust.jcommander.converters.IntegerConverter;
import com.cloudera.science.ml.avro.MLCenters;
import com.cloudera.science.ml.client.params.InputParameters;
import com.cloudera.science.ml.client.params.PipelineParameters;
import com.cloudera.science.ml.client.util.AvroIO;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.vectors.VectorConvert;
import com.cloudera.science.ml.kmeans.parallel.KMeansParallel;
import com.cloudera.science.ml.parallel.types.MLRecords;
import com.google.common.collect.Lists;

@Parameters(commandDescription =
    "Apply a set of centers to a dataset and output the resulting assignments/distances")
public class KMeansAssignmentCommand implements Command {

  @Parameter(names = "--centers-file", required=true,
      description = "The local Avro file containing the centers to be applied")
  private String centersFile;
  
  @Parameter(names = "--center-ids",
      description = "A CSV containing the indices of the centers to use for the assignment",
      splitter = CommaParameterSplitter.class,
      converter = IntegerConverter.class)
  private List<Integer> centerIds = Lists.newArrayList();
  
  @Parameter(names = "--output-path", required=true,
      description = "The path to write the CSV output to (format: id, clustering_id, center_id, distance)")
  private String assignmentsPath;
  
  @Parameter(names = "--output-delim",
      description = "The delimiter to use for the CSV assignment output")
  private String outputDelim = ",";
  
  @ParametersDelegate
  private InputParameters inputParams = new InputParameters();
  
  @ParametersDelegate
  private PipelineParameters pipelineParams = new PipelineParameters();
  
  @Override
  public int execute(Configuration conf) throws Exception {
    Pipeline p = pipelineParams.create(KMeansAssignmentCommand.class, conf);
    PCollection<Vector> input = inputParams.getVectors(p);
    List<MLCenters> centers = AvroIO.read(MLCenters.class, new File(centersFile));
    if (!centerIds.isEmpty()) {
      List<MLCenters> filter = Lists.newArrayListWithExpectedSize(centerIds.size());
      for (int i = 0; i < centerIds.size(); i++) {
        filter.add(centers.get(centerIds.get(i)));
      }
      centers = filter;
    }
    KMeansParallel kmp = new KMeansParallel();

    PType<Record> recordType = MLRecords.csvRecord(WritableTypeFamily.getInstance(),
        outputDelim);
    PCollection<Record> assigned = kmp.computeClusterAssignments(input,
        Lists.transform(centers, VectorConvert.TO_CENTERS), recordType);
    p.write(assigned, To.textFile(assignmentsPath));
    p.done();
    return 0;
  }

  @Override
  public String getDescription() {
    return "Apply a set of centers to a dataset and output the resulting assignments/distances";
  }

}
