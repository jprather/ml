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
package com.cloudera.science.ml.core.records.avro;

import java.util.List;

import org.apache.avro.Schema;

import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Spec;
import com.google.common.collect.Lists;

/**
 *
 */
public class Spec2Schema {

  public static Schema create(Spec spec) {
    if (DataType.RECORD == spec.getDataType()) {
      List<Schema.Field> fields = Lists.newArrayList();
      for (int i = 0; i < spec.size(); i++) {
        FieldSpec f = spec.getField(i);
        fields.add(new Schema.Field(f.name(), create(f.spec()), "", null));
      }
      Schema s = Schema.createRecord("R" + spec.hashCode(), "", "", false);
      s.setFields(fields);
      return s;
    } else {
      switch (spec.getDataType()) {
      case INT:
        return Schema.create(Schema.Type.INT);
      case LONG:
        return Schema.create(Schema.Type.LONG);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case STRING:
        return Schema.create(Schema.Type.STRING);
      }
      return null;
    }
  }
  
  private Spec2Schema() {}
}
