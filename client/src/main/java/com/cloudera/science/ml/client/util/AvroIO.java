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
package com.cloudera.science.ml.client.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.google.common.collect.Lists;

/**
 *
 */
public class AvroIO {
  
  public static <T extends SpecificRecord> void write(List<T> values, File file)
      throws IOException {
    Schema s = values.get(0).getSchema();
    Class clazz = values.get(0).getClass();
    DataFileWriter<T> dfw = new DataFileWriter<T>(new SpecificDatumWriter<T>(clazz));
    dfw.create(s, file);
    for (T t : values) {
      dfw.append(t);
    }
    dfw.close();
  }
  
  public static <T extends SpecificRecord> List<T> read(Class<T> clazz, File file)
      throws IOException {
    DatumReader<T> dr = new SpecificDatumReader<T>(clazz);
    DataFileReader<T> dfr = new DataFileReader<T>(file, dr);
    List<T> values = Lists.newArrayList();
    while (dfr.hasNext()) {
      values.add(dfr.next());
    }
    return values;
  }
}
