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

import java.io.File;

import com.beust.jcommander.ParametersDelegate;
import com.cloudera.science.ml.parallel.normalize.Summary;

/**
 *
 */
public class SummaryParameters {

  @ParametersDelegate
  private JacksonParameters jacksonParams = new JacksonParameters();
  
  public void save(Summary summary, String summaryFile) {
    jacksonParams.save(summary, new File(summaryFile));
  }
  
  public Summary get(String summaryFile) {
    return jacksonParams.read(new File(summaryFile), Summary.class);
  }
}
