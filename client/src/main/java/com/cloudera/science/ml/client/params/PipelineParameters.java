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
package com.cloudera.science.ml.client.params;

import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.Parameter;

/**
 *
 */
public class PipelineParameters {
  
  @Parameter(names = "--local",
      description = "Use the local, in-memory pipeline implementation")
  private boolean inMemory = false;
  
  public Pipeline create(Class<?> jarClass, Configuration conf) {
    return inMemory ? MemPipeline.getInstance() : new MRPipeline(jarClass, conf);
  }
}
