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
