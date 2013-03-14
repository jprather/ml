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
package com.cloudera.science.ml.kmeans.parallel;

import com.cloudera.science.ml.core.Centers;
import com.cloudera.science.ml.core.Vectors;
import com.cloudera.science.ml.core.Weighted;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.Vector;

/**
 * An internal data structure that manages the locations of the current centers during
 * k-means|| processing.
 */
class CentersIndex implements Serializable {
  private final int[] pointsPerCenter;
  private final Map<double[], Data> index;
  
  public static class Distances {
    public double[] clusterDistances;
    public int[] closestPoints;
    
    public Distances(double[] clusterDistances, int[] closestPoints) {
      this.clusterDistances = clusterDistances;
      this.closestPoints = closestPoints;
    }
  }
  
  private static class Data implements Serializable {
    double lengthSquared;
    Map<Integer, Integer> clusterCenters;
    
    public Data(double[] key) {
      this.lengthSquared = 0;
      for (double d : key) {
        lengthSquared += d * d;
      }
      this.clusterCenters = Maps.newLinkedHashMap();
    }
    
    public boolean hasClusterId(int clusterId) {
      return clusterCenters.containsKey(clusterId);
    }
    
    public void add(int clusterId, int centerId) {
      this.clusterCenters.put(clusterId, centerId);
    }
  }
  
  public CentersIndex(int numCenters) {
    this.pointsPerCenter = new int[numCenters];
    this.index = Maps.newHashMap();
  }
  
  public CentersIndex(List<Centers> centers) {
    this(centers.size());
    for (int centerId = 0; centerId < centers.size(); centerId++) {
      for (Vector v : centers.get(centerId)) {
        add(Vectors.toArray(v), centerId);
      }
    }
  }
  
  public int getNumCenters() {
    return pointsPerCenter.length;
  }

  public int[] getPointsPerCluster() {
    return pointsPerCenter;
  }
  
  public void add(double[] v, Integer... centerIds) {
    add(v, Arrays.asList(centerIds));
  }
  
  public void add(double[] v, Iterable<Integer> centerIds) {
    Data data = index.get(v);
    if (data == null) {
      data = new Data(v);
      index.put(v, data);
    }
    for (Integer centerId : centerIds) {
      if (!data.hasClusterId(centerId)) {
        int pointId = pointsPerCenter[centerId];
        data.add(centerId, pointId);
        pointsPerCenter[centerId]++;
      }
    }
  }
  
  public Distances getDistances(Vector vec) {
    int[] closestPoints = new int[pointsPerCenter.length];
    double[] distances = new double[pointsPerCenter.length];
    Arrays.fill(distances, Double.POSITIVE_INFINITY);
    double lenSq = vec.getLengthSquared();
    for (Map.Entry<double[], Data> e : index.entrySet()) {
      double dist = e.getValue().lengthSquared + lenSq - 2.0 * dot(e.getKey(), vec);
      for (Map.Entry<Integer, Integer> entry : e.getValue().clusterCenters.entrySet()) {
        if (dist < distances[entry.getKey()]) {
          distances[entry.getKey()] = dist;
          closestPoints[entry.getKey()] = entry.getValue();
        }
      }
    }
    return new Distances(distances, closestPoints);
  }
  
  public List<List<Weighted<Vector>>> getWeightedVectors(List<List<Long>> pointCounts) {
    List<List<Weighted<Vector>>> ret = Lists.newArrayList();
    for (int i = 0; i < pointCounts.size(); i++) {
      ret.add(Arrays.<Weighted<Vector>>asList(new Weighted[pointCounts.get(i).size()]));
    }
    for (Map.Entry<double[], Data> e  : index.entrySet()) {
      Vector v = Vectors.of(e.getKey());
      for (Map.Entry<Integer, Integer> entry : e.getValue().clusterCenters.entrySet()) {
        long weight = pointCounts.get(entry.getKey()).get(entry.getValue());
        ret.get(entry.getKey()).set(entry.getValue(), new Weighted<Vector>(v, weight));
      }
    }
    return ret;
  }
  
  private static double dot(double[] center, Vector vec) {
    double prod = 0.0;
    for (Vector.Element e : vec) {
      prod += e.get() * center[e.index()];
    }
    return prod;
  }
}
