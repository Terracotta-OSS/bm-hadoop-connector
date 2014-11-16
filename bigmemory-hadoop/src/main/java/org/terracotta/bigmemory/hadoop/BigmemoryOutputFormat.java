package org.terracotta.bigmemory.hadoop;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.terracotta.bigmemory.hadoop.util.HadoopCompatabilityUtil;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * Convert Map/Reduce output and write it to Clustered Ehcache.
 * The output value <u>must</u> be a {@link net.sf.ehcache.Element} instance.
 *
 */

public class BigmemoryOutputFormat extends OutputFormat<Writable, Writable> {

  /**
   * Check for validity of the output-specification for the job.
   *
   * <p>This is to validate the output specification for the job when it is
   * a job is submitted.  Typically checks that it does not already exist,
   * throwing an exception when it already exists, so that output is not
   * overwritten.</p>
   *
   * @param context information about the job
   * @throws java.io.IOException when output should not be attempted
   */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
  }

  /**
   * Returns the output committer.
   *
   * @param context  The current context.
   * @return The committer.
   * @throws java.io.IOException When creating the committer fails.
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.OutputFormat#getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new BigmemoryOutputCommitter();
  }

  @Override
  public BigmemoryRecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration configuration = HadoopCompatabilityUtil.getConfiguration(context);
    URL ehcacheUrl = configuration.getResource(BigmemoryHadoopConfiguration.EHCACHE_CONFIG_XML);
    String cacheName = configuration.get(BigmemoryHadoopConfiguration.BM_OUTPUT_CACHE);
    boolean doBulkLoad = configuration.getBoolean(BigmemoryHadoopConfiguration.BM_BULK_LOAD, false);
    boolean useUnboundedPool = configuration.getBoolean(BigmemoryHadoopConfiguration.BM_THREADS, false);
    // Default 2 * num_CPU for I/O bound tasks, 1 + num_CPU for CPU bound tasks. We're network bound.
    int numPutThreads = Runtime.getRuntime().availableProcessors() * 2;
    if (!useUnboundedPool) {
      numPutThreads  = configuration.getInt(BigmemoryHadoopConfiguration.BM_COMMIT_THREADS, numPutThreads);
    }
    return new BigmemoryRecordWriter(ehcacheUrl, cacheName, doBulkLoad, useUnboundedPool, numPutThreads);
  }


  /**
   * Sets the name of the cache which will receive the output records
   */
  public static void setOutputCache(Job job, String cache) {
    job.getConfiguration().set(BigmemoryHadoopConfiguration.BM_OUTPUT_CACHE, cache );
  }

  protected static class BigmemoryRecordWriter extends RecordWriter<Writable, Writable> {

    private ExecutorService service;

    private final Log log = LogFactory.getLog(BigmemoryRecordWriter.class);

    private CacheManager ehcacheManager;
    private Cache cache;
    private boolean doBulkLoad;

    /**
     * Instantiate an BigmemoryRecordWriter with the Cache for writing
     * @param cacheName The Cache used for storing map reduce outputs
     *
     */
    public BigmemoryRecordWriter(URL ehcacheUrl, String cacheName, boolean isBulkLoad, boolean useUnboundedThreadPool, int maxPutThreads) throws IOException {
      this.ehcacheManager = CacheManager.create(ehcacheUrl);
      checkConfiguration(ehcacheManager, cacheName);
      this.cache = ehcacheManager.getCache(cacheName);
      this.doBulkLoad = isBulkLoad;
      if (doBulkLoad) {
        cache.setNodeBulkLoadEnabled(true);
      }

      if (useUnboundedThreadPool) {
        this.service = Executors.newCachedThreadPool();
      } else {
        this.service = Executors.newFixedThreadPool(maxPutThreads);
      }
    }

    @Override
    public void write(Writable key, Writable value) throws IOException,
        InterruptedException {
      if (value instanceof BigmemoryElementWritable) {
        final Element element = ((BigmemoryElementWritable)value).getElement();
        Runnable task = new Runnable() {
          public void run() {
            cache.put(element);
          }
        };
        service.execute(task);
      } else {
        throw new IOException("Expecting an Element but got " + value.getClass().getSimpleName());
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      if (doBulkLoad) {
        cache.setNodeBulkLoadEnabled(false);
      }
      service.shutdown();
      service.awaitTermination(5, TimeUnit.MINUTES);
      ehcacheManager.shutdown();
    }

    private void checkConfiguration(CacheManager mgr, String cache) throws IOException {
      if (!mgr.cacheExists(cache)) {
        throw new IOException("Cache " + cache + " not found in the supplied Ehcache configuration.");
      }
      if (mgr.getConfiguration().getMaxBytesLocalHeap() > 100 * 1024 * 1024) {
        log.warn("Detected large on-heap memory allocated to Cache Manager: " + mgr.getConfiguration().getMaxBytesLocalHeap() +
            " . Your reducers might run out of memory. Increase per task memory using the \"mapred.child.java.opts\" property.");
      }
      if(mgr.getConfiguration().getTerracottaConfiguration() == null) {
        throw new IOException("You need Bigmemory Max to use the BigmemoryOutputFormat. Please check the Bigmemory configuration.");
      }
    }

  }

}
