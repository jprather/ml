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

import java.util.List;

import com.cloudera.science.ml.client.cmd.CommandException;
import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Spec;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class Specs {
  
  public static Integer getFieldId(Spec spec, String value) {
    List<Integer> fieldIds = getFieldIds(spec, ImmutableList.of(value));
    if (fieldIds.isEmpty()) {
      throw new CommandException("Could not find field " + value + " in spec");
    }
    return fieldIds.get(0);
  }
  
  public static List<Integer> getFieldIds(Spec spec, List<String> values) {
    if (values.isEmpty()) {
      return ImmutableList.of();
    }
    
    List<Integer> fieldIds = null;
    if (spec == null || spec.getField(values.get(0)) == null) {
      fieldIds = Lists.transform(values, new Function<String, Integer>() {
        @Override
        public Integer apply(String input) {
          return Integer.valueOf(input);
        }
      });
    } else {
      fieldIds = Lists.newArrayListWithExpectedSize(values.size());
      for (int i = 0; i < values.size(); i++) {
        FieldSpec f = spec.getField(values.get(i));
        if (f != null) {
          fieldIds.add(f.position());
        }
      }
    }
    return fieldIds;
  }
}
