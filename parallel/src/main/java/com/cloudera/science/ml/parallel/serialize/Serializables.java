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
package com.cloudera.science.ml.parallel.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

/**
 * Utilities for working with {@code Serializable} object types in Crunch.
 */
public class Serializables {
  
  public static final <T extends Serializable>  PType<T> ptype(Class<T> clazz, PTypeFamily ptf) {
    return ptf.derived(clazz, new InFn<T>(clazz), new OutFn<T>(), ptf.bytes());
  }

  private static class InFn<T> extends MapFn<ByteBuffer, T> {
    private final Class<T> clazz;
    
    public InFn(Class<T> clazz) {
      this.clazz = clazz;
    }
    
    @Override
    public T map(ByteBuffer input) {
      ByteArrayInputStream bais = new ByteArrayInputStream(input.array(), input.position(), input.limit());
      try {
        ObjectInputStream ois = new ObjectInputStream(bais);
        return clazz.cast(ois.readObject());
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  private static class OutFn<T> extends MapFn<T, ByteBuffer> {
    @Override
    public ByteBuffer map(T input) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(input);
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
      return ByteBuffer.wrap(baos.toByteArray());
    }
  }
}
