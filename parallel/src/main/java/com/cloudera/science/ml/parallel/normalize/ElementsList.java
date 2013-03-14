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
package com.cloudera.science.ml.parallel.normalize;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

import com.google.common.collect.Lists;

/**
 *
 */
public class ElementsList extends Elements {

  private ArrayList<Element> backing;

  public ElementsList(Collection<Element> backing) {
    if (backing instanceof ArrayList) {
      this.backing = (ArrayList<Element>) backing;
    } else {
      this.backing = Lists.newArrayList(backing);
    }
  }

  @Override
  public Element get(int index) {
    return backing.get(index);
  }

  @Override
  public int size() {
    return backing.size();
  }
  
  private static MapFn<Collection<Element>, Elements> IN = new MapFn<Collection<Element>, Elements>() {
    @Override
    public Elements map(Collection<Element> in) {
      return new ElementsList(in);
    }
  };
  
  private static MapFn<Elements, Collection<Element>> OUT = new MapFn<Elements, Collection<Element>>() {
    @Override
    public Collection<Element> map(Elements in) {
      if (in instanceof ElementsList) {
        return ((ElementsList) in).backing;
      } else {
        return Lists.newArrayList(in);
      }
    }
  };

  public static PType<Elements> PTYPE = Avros.derived(Elements.class, IN, OUT,
      Avros.collections(Element.PTYPE));
}
