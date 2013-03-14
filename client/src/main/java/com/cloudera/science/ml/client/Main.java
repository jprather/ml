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
package com.cloudera.science.ml.client;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cloudera.science.ml.client.cmd.Command;
import com.cloudera.science.ml.client.cmd.KMeansCommand;
import com.cloudera.science.ml.client.cmd.StandardizeCommand;
import com.cloudera.science.ml.client.cmd.SampleCommand;
import com.cloudera.science.ml.client.cmd.SummaryCommand;
import com.google.common.collect.ImmutableSortedMap;

public class Main extends Configured implements Tool {
  
  private JCommander jc;
  private Help help = new Help();
  
  private static final Map<String, Command> COMMANDS = ImmutableSortedMap.<String, Command>naturalOrder()
      .put("sample", new SampleCommand())
      .put("summary", new SummaryCommand())
      .put("standardize", new StandardizeCommand())
      .put("kmeans", new KMeansCommand())
      .build();
  
  public Main() {
    jc = new JCommander(this);
    jc.addCommand("help", help, "-help", "--help");
    for (Map.Entry<String, Command> e : COMMANDS.entrySet()) {
      jc.addCommand(e.getKey(), e.getValue());
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    try {
      jc.parse(args);
    } catch (ParameterException pe) {
      System.err.println(pe.getMessage());
      return 1;
    }
    
    if ("help".equals(jc.getParsedCommand())) {
      return help.usage(jc, COMMANDS);
    }
    
    Command cmd = COMMANDS.get(jc.getParsedCommand());
    if (cmd == null) {
      return help.usage(jc, COMMANDS);
    }
    return cmd.execute(getConf());
  }
  
  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new Main(), args);
    System.exit(rc);
  }
}
