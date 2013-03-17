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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.beust.jcommander.converters.IntegerConverter;
import com.cloudera.science.ml.avro.MLCenters;
import com.cloudera.science.ml.avro.MLWeightedCenters;
import com.cloudera.science.ml.client.params.RandomParameters;
import com.cloudera.science.ml.client.util.AvroIO;
import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.VectorConvert;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.cloudera.science.ml.kmeans.core.KMeans;
import com.cloudera.science.ml.kmeans.core.KMeansInitStrategy;
import com.cloudera.science.ml.kmeans.core.PredictionStrength;
import com.cloudera.science.ml.kmeans.core.StoppingCriteria;
import com.google.common.collect.Lists;

@Parameters(commandDescription = "Executes k-means++ on Avro vectors stored on the local filesystem")
public class KMeansCommand implements Command {

  @Parameter(names = "--input-sketches", required=true,
      description = "The local Avro file that contains the sketches computed by the ksketch command")
  private String sketchFile;
  
  @Parameter(names = "--clusters",
      description = "A CSV of ints that specifies the number of clusters to create",
      required = true,
      converter = IntegerConverter.class,
      splitter = CommaParameterSplitter.class)
  List<Integer> clusters = Lists.newArrayList();

  @Parameter(names = "--init-strategy",
      description = "The k-means initialization strategy (PLUS_PLUS or RANDOM)")
  private String initStrategyName = KMeansInitStrategy.PLUS_PLUS.name();

  @Parameter(names = "--max-iterations",
      description = "The maximum number of Lloyd's iterations to run")
  private int maxLloydsIterations = 100;  

  @Parameter(names = "--stopping-threshold",
      description = "Stop the Lloyd's iterations if the delta between centers falls below this")
  private double stoppingThreshold = 1e-4;  

  @Parameter(names = "--output-centers",
      description = "A local file to store the centers that were created into")
  private String centersOutputFile;
  
  @ParametersDelegate
  private RandomParameters randomParams = new RandomParameters();

  @Override
  public String getDescription() {
    return "Executes k-means++ on Avro vectors stored on the local filesystem";
  }
  
  @Override
  public int execute(Configuration conf) throws Exception {
    KMeansInitStrategy initStrategy = KMeansInitStrategy.valueOf(initStrategyName);
    KMeans kmeans = new KMeans(initStrategy, getStoppingCriteria());
    Random rand = randomParams.getRandom();
    
    List<MLWeightedCenters> mlwc = AvroIO.read(MLWeightedCenters.class, new File(sketchFile));
    List<List<Weighted<Vector>>> sketches = toSketches(mlwc);
    List<Weighted<Vector>> allPoints = Lists.newArrayList();
    for (int i = 0; i < sketches.size(); i++) {
      allPoints.addAll(sketches.get(i));
    }
    List<Centers> centers = getClusters(allPoints, kmeans, rand);
    AvroIO.write(Lists.transform(centers, VectorConvert.FROM_CENTERS),
        new File(centersOutputFile));
    
    if (sketches.size() > 1) {
      // Perform the prediction strength calculations
      List<Weighted<Vector>> train = Lists.newArrayList();
      for (int i = 0; i < sketches.size() - 1; i++) {
        train.addAll(sketches.get(i));
      }
      List<Weighted<Vector>> test = sketches.get(sketches.size() - 1);
      List<Centers> trainCenters = getClusters(train, kmeans, rand);
      List<Centers> testCenters = getClusters(test, kmeans, rand);
      PredictionStrength ps = new PredictionStrength(testCenters, test, trainCenters);
      List<Double> psScores = ps.computeScores();
      System.out.println("Prediction Strengths");
      for (int i = 0; i < psScores.size(); i++) {
        System.out.println(String.format("%d Clusters: %.6f",
            trainCenters.get(i).size(), psScores.get(i)));
      }
    }
    
    return 0;
  }
  
  private List<Centers> getClusters(List<Weighted<Vector>> sketch, KMeans kmeans, Random rand) {
    List<Centers> centers = Lists.newArrayList();
    for (Integer nc : clusters) {
      centers.add(kmeans.compute(sketch, nc, rand));
    }
    return centers;
  }
  
  private List<List<Weighted<Vector>>> toSketches(List<MLWeightedCenters> mlwc) {
    List<List<Weighted<Vector>>> base = Lists.newArrayList();
    for (MLWeightedCenters wc : mlwc) {
      base.add(Lists.transform(wc.getCenters(), VectorConvert.TO_WEIGHTED_VEC));
    }
    return base;
  }
  
  private StoppingCriteria getStoppingCriteria() {
    return StoppingCriteria.or(StoppingCriteria.threshold(stoppingThreshold),
        StoppingCriteria.maxIterations(maxLloydsIterations));
  }
}
