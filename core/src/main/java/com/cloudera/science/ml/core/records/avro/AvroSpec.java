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
import org.apache.avro.Schema.Field;

import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Spec;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 *
 */
public class AvroSpec implements Spec {

  private final Schema schema;
  private final DataType dataType;
  
  private static DataType getDataType(Schema schema) {
    Schema.Type st = schema.getType();
    // TODO: Null check
    switch (st) {
    case RECORD:
      return DataType.RECORD;
    case INT:
      return DataType.INT;
    case BOOLEAN:
      return DataType.BOOLEAN;
    case FLOAT:
    case DOUBLE:
      return DataType.DOUBLE;
    case STRING:
      return DataType.STRING;
    case LONG:
      return DataType.LONG;
      default:
        throw new IllegalStateException("Cannot support schema type = " + st);
    }
  }
  
  public AvroSpec(Schema schema) {
    this.schema = schema;
    this.dataType = getDataType(schema);
  }
  
  public Schema getSchema() {
    return schema;
  }

  @Override
  public int size() {
    return schema.getFields().size();
  }

  @Override
  public List<String> getFieldNames() {
    return Lists.transform(schema.getFields(), new Function<Schema.Field, String>() {
      @Override
      public String apply(Field input) {
        return input.name();
      }
    });
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public FieldSpec getField(String fieldName) {
    return new AvroFieldSpec(schema.getField(fieldName));
  }

  @Override
  public FieldSpec getField(int index) {
    return new AvroFieldSpec(schema.getFields().get(index));
  }
}
