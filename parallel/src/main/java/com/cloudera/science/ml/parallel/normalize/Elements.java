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
package com.cloudera.science.ml.parallel.normalize;

import java.util.AbstractList;

import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.Vectors;

/**
 *
 */
public abstract class Elements extends AbstractList<Element> {
  public Vector asVector(double[] values, boolean sparse) {
    if (sparse) {
      return Vectors.sparse(values.length).assign(values);
    } else {
      return Vectors.of(values);
    }
  }
}
