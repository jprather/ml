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

import java.util.List;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.cloudera.science.ml.client.cmd.Command;
import com.google.common.collect.Lists;

/**
 *
 */
@Parameters(commandDescription = "Retrieves details on the functions of other commands")
public class Help {
  @Parameter(description = "Commands")
  List<String> helpCommands = Lists.newArrayList();;
  
  public int usage(JCommander jc, Map<String, Command> cmds) {
    if (helpCommands.isEmpty()) {
      System.out.println("Commands:\n");
      for (Map.Entry<String, Command> e : cmds.entrySet()) {
        System.out.println(String.format("\t%s\t\t%s", e.getKey(), e.getValue().getDescription()));
      }
      System.out.println("\nYou can get detailed help on any individual command by typing 'help <cmd>'");
    } else {
      for (String cmd : helpCommands) {
        jc.usage(cmd);
      }
    }
    return 0;
  }
}
