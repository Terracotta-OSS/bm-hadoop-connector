/**
 * Copyright 2014 Terracotta Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/**
 *
 */
package org.terracotta.bigmemory.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class BigmemoryOutputCommitter extends OutputCommitter {

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.OutputCommitter#setupJob(org.apache.hadoop.mapreduce.JobContext)
   *
   * Set up the ehcache client here for receiving the k-v pairs
   */
  @Override
  public void setupJob(JobContext jobContext) throws IOException {

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.OutputCommitter#cleanupJob(org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public void cleanupJob(JobContext context) throws IOException {
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.OutputCommitter#setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
   *
   * Can leave this no-op for now
   */
  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.OutputCommitter#needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext)
   * No need to do anything per task level
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.OutputCommitter#commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
   *
   * This will not be called since needsTaskCommit() is returning false
   */
  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {


  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.OutputCommitter#abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
  }

}
