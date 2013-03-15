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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.io.To;
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
import com.cloudera.science.ml.client.params.RandomParameters;
import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.VectorConvert;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.cloudera.science.ml.kmeans.core.KMeans;
import com.cloudera.science.ml.kmeans.core.KMeansInitStrategy;
import com.cloudera.science.ml.kmeans.core.StoppingCriteria;
import com.cloudera.science.ml.kmeans.parallel.KMeansParallel;
import com.google.common.collect.Lists;

/**
 *
 */
@Parameters(commandDescription = "Executes k-means++ and scalable k-means++ (a.k.a. k-means||)")
public class KMeansCommand implements Command {

  @Parameter(names = "--clusters",
      description = "A CSV of ints that specifies the number of clusters to create",
      required = true,
      converter = IntegerConverter.class,
      splitter = CommaParameterSplitter.class)
  List<Integer> numClusters = Lists.newArrayList();

  @Parameter(names = "--init-vectors-file", required=true,
      description = "The file that contains the vector(s) used for initializing k-means||")
  private String initVectorsPath;
  
  @Parameter(names = "--iterations",
      description = "The number of iterations of k-means|| to run")
  private int numIterations = 5;
  
  @Parameter(names = "--scale-factor",
      description = "The scale factor to use for sampling points in k-means||")
  private int scaleFactor = 2;
  
  @Parameter(names = "--init-strategy",
      description = "The k-means initialization strategy (PLUS_PLUS or RANDOM)")
  private String initStrategyName = KMeansInitStrategy.PLUS_PLUS.name();
  
  @Parameter(names = "--clusters-output",
      description = "Save the clusters that were found to this local Avro file",
      required = true)
  private String saveClustersTo;
  
  @Parameter(names = "--assignments-output",
      description = "Saves the cluster assignments of the points to an Avro file in HDFS")
  private String clusterAssignmentsOutput;
  
  @ParametersDelegate
  private PipelineParameters pipelineParams = new PipelineParameters();
  
  @ParametersDelegate
  private InputParameters inputParams = new InputParameters();

  @ParametersDelegate
  private RandomParameters randomParams = new RandomParameters();

  // Parameters that are left outside of the user's control for now
  private int maxLloydsIterations = 100;  
  private double stoppingThreshold = 1e-4;
  
  @Override
  public String getDescription() {
    return "Executes k-means++ and scalable k-means++ (a.k.a. k-means||)";
  }
  
  @Override
  public int execute(Configuration conf) throws Exception {
    Pipeline p = pipelineParams.create(KMeansCommand.class, conf);
    PCollection<Vector> input = inputParams.getVectors(p);

    KMeansParallel kmp = new KMeansParallel(randomParams.getRandom());
    KMeansInitStrategy initStrategy = KMeansInitStrategy.valueOf(initStrategyName);
    Vector initialPoint = getInitialVector(p);
    StoppingCriteria stoppingCriteria = getStoppingCriteria();
    KMeans kmeans = new KMeans(initStrategy, stoppingCriteria);
    Integer maxClusters = Collections.max(numClusters);
    
    List<Weighted<Vector>> sketch = kmp.initialization(input,
        numIterations, maxClusters * scaleFactor, initialPoint);
    List<Centers> centers = Lists.newArrayList();
    for (Integer nc : numClusters) {
      centers.add(kmeans.compute(sketch, nc, randomParams.getRandom()));
    }
    List<Double> clusteringCosts = kmp.getCosts(input, centers).getValue();
    for (int i = 0; i < clusteringCosts.size(); i++) {
      System.out.println(String.format("Cost of using %d clusters = %.4f",
          numClusters.get(i), clusteringCosts.get(i)));
    }
    // Save the centers to a local file
    saveCentersToFile(centers);
    
    // Optionally write out the assignments of points to clusters
    if (clusterAssignmentsOutput != null) {
      kmp.computeClusterAssignments(input, centers).write(
          To.avroFile(clusterAssignmentsOutput),WriteMode.OVERWRITE);
    }
    
    p.done();
    return 0;
  }
  
  private Vector getInitialVector(Pipeline pipeline) {
    PCollection<Vector> initVectors = inputParams.getVectorsFromPath(pipeline, initVectorsPath);
    Vector ret = null;
    Random r = new Random();
    double prob = 1.0;
    int count = 1;
    for (Vector v : initVectors.materialize()) {
      if (r.nextDouble() < prob) {
        ret = v;
      }
      count++;
      prob = 1.0 / count;
    }
    if (ret == null) {
      throw new IllegalArgumentException("No vectors read from: " + initVectorsPath);
    }
    return ret;
  }
  
  private void saveCentersToFile(List<Centers> centers) throws Exception {
    // Should consider adding a text option here
    SpecificDatumWriter<MLCenters> sdw = new SpecificDatumWriter<MLCenters>(MLCenters.class);
    DataFileWriter<MLCenters> dfw = new DataFileWriter<MLCenters>(sdw);
    dfw.create(MLCenters.SCHEMA$, new File(saveClustersTo));
    for (MLCenters mlc : Lists.transform(centers, VectorConvert.FROM_CENTERS)) {
      dfw.append(mlc);
    }
    dfw.close();
  }
  
  private StoppingCriteria getStoppingCriteria() {
    return StoppingCriteria.or(StoppingCriteria.threshold(stoppingThreshold),
        StoppingCriteria.maxIterations(maxLloydsIterations));
  }
}
