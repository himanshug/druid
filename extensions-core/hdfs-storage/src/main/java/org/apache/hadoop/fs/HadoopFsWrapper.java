/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs;

import com.google.common.base.Throwables;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This wrapper class is created to be able to access some of the the "protected" methods inside Hadoop's
 * FileSystem class. Those are supposed to become public eventually or more appropriate alternatives would be
 * provided.
 * This is a hack and should be removed when no longer necessary.
 */
public class HadoopFsWrapper
{
  private static final Logger log = new Logger(HadoopFsWrapper.class);

  private HadoopFsWrapper() {}

  /**
   * Same as FileSystem.rename(from, to, Options.Rename). It is different from FileSystem.rename(from, to) which moves
   * "from" directory inside "to" directory if it already exists.
   *
   * @param from
   * @param to
   * @param replaceExisting if existing files should be overwritten
   *
   * @return true if operation succeeded, false if replaceExisting == false and destination already exists
   *
   * @throws IOException if trying to overwrite a non-empty directory
   */
  public static boolean rename(FileSystem fs, Path from, Path to, boolean replaceExisting) throws IOException
  {
    try {
      // Note: Using reflection instead of simpler
      // fs.rename(from, to, replaceExisting ? Options.Rename.OVERWRITE : Options.Rename.NONE);
      // due to the issues discussed in https://github.com/druid-io/druid/pull/3787
      Method renameMethod = fs.getClass().getMethod("rename", Path.class, Path.class, Options.Rename[].class);
      renameMethod.invoke(fs, from, to, new Options.Rename[]{Options.Rename.NONE});
      return true;
    }
    catch (InvocationTargetException ex) {
      if (ex.getTargetException() instanceof FileAlreadyExistsException) {
        log.info(ex, "Destination exists while renaming [%s] to [%s]", from, to);
        return false;
      } else {
        throw Throwables.propagate(ex);
      }
    }
    catch (NoSuchMethodException | IllegalAccessException ex) {
      throw Throwables.propagate(ex);
    }
  }
}
