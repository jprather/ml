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

import com.cloudera.science.ml.core.records.Spec;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 *
 */
public class Specs {
  
  public static Integer getFieldId(Spec spec, String value) {
    return getFieldIds(spec, ImmutableList.of(value)).get(0);
  }
  
  public static List<Integer> getFieldIds(Spec spec, List<String> values) {
    List<Integer> fieldIds = null;
    if (spec == null || spec.getField(values.get(0)) == null) {
      fieldIds = Lists.transform(values, new Function<String, Integer>() {
        @Override
        public Integer apply(String input) {
          return Integer.valueOf(input);
        }
      });
    } else {
      fieldIds = Lists.newArrayListWithExpectedSize(spec.size());
      for (int i = 0; i < spec.size(); i++) {
        fieldIds.add(spec.getField(values.get(i)).position());
      }
    }
    return fieldIds;
  }
}
