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

import org.apache.crunch.PCollection;
import org.apache.crunch.Target;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.mahout.math.Vector;

import com.beust.jcommander.Parameter;
import com.cloudera.science.ml.client.cmd.CommandException;
import com.cloudera.science.ml.mahout.types.MLWritables;
import com.cloudera.science.ml.parallel.types.MLAvros;

public class OutputParameters {

  @Parameter(names = "--output-type",
      description = "One of 'avro' or 'seq', for Avro or SequenceFile output files", required=true)
  private String outputType;
  
  public PType<Vector> getVectorPType() {
    outputType = outputType.toLowerCase();
    if ("avro".equals(outputType)) {
      return MLAvros.vector();
    } else if ("seq".equals(outputType)) {
      return MLWritables.vector();
    } else {
      throw new CommandException("Unsupported Vector output type: " + outputType);
    }
  }
  
  public <T> void write(PCollection<T> collect, String output) {
    outputType = outputType.toLowerCase();
    PTypeFamily ptf = collect.getTypeFamily();
    PType<T> ptype = collect.getPType();
    Target target = null;
    if ("text".equals(outputType)) {
      target = To.textFile(output);
    } else if ("avro".equals(outputType)) {
      if (AvroTypeFamily.getInstance() != ptf) {
        // Attempt to force conversion
        ptype = AvroTypeFamily.getInstance().as(ptype);
        if (ptype == null) {
          forceConversionException(output, ptype, "avro");
        }
        collect = collect.parallelDo(IdentityFn.<T>getInstance(), ptype);
      }
      target = At.avroFile(output, (AvroType<T>) ptype);
    } else if ("seq".equals(outputType)) {
      if (WritableTypeFamily.getInstance() != ptf) {
        ptype = WritableTypeFamily.getInstance().as(ptype);
        if (ptype == null) {
          forceConversionException(output, ptype, "seq");
        }
        collect = collect.parallelDo(IdentityFn.<T>getInstance(), ptype);
      }
      target = At.sequenceFile(output, ptype);
    } else {
      throw new CommandException("Unknown output type: " + outputType);
    }
    collect.write(target, WriteMode.OVERWRITE);
  }
  
  private void forceConversionException(String outputFile, PType<?> ptype, String type) {
    String msg = String.format(
        "Could not convert type %s into %s format for output: %s",
        ptype.getTypeClass().getCanonicalName(), type, outputFile);
    throw new CommandException(msg);
  }
}
