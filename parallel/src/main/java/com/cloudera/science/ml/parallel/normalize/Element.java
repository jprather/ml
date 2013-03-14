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

import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

/**
 *
 */
public class Element {
  
  public static PType<Element> PTYPE = Avros.reflects(Element.class);
  
  private int fieldId;
  private String strValue;
  private double numValue;
  
  @SuppressWarnings("unused")
  private Element() {
    this(-1, null, Double.NaN);
  }
  
  public Element(int fieldId, String strValue) {
    this(fieldId, strValue, Double.NaN);
  }
  
  public Element(int fieldId, double numValue) {
    this(fieldId, null, numValue);
  }
  
  private Element(int fieldId, String strValue, double numValue) {
    this.fieldId = fieldId;
    this.strValue = strValue;
    this.numValue = numValue;
  }
  
  public boolean isValid() {
    return fieldId >= 0;
  }
  
  public int index() {
    return fieldId;
  }
  
  public double getNumeric() {
    if (!Double.isNaN(numValue)) {
      return numValue;
    } else {
      return Double.valueOf(strValue);
    }
  }
  
  public String getSymbolic() {
    if (strValue != null) {
      return strValue;
    } else {
      return String.valueOf(numValue);
    }
  }
}
