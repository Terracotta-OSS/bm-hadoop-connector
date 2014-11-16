package org.terracotta.bigmemory.hadoop;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Assert;



public class TestBigMemoryOutputFormat extends TestCase  {

	private static final Log LOG = LogFactory.getLog(TestBigMemoryOutputFormat.class);
	
	public void testOutputCommiter() {
		Configuration conf = new Configuration();
		conf.set(BigmemoryHadoopConfiguration.BM_OUTPUT_CACHE, "test");
		conf.setBoolean(BigmemoryHadoopConfiguration.BM_BULK_LOAD, false);
		conf.setBoolean(BigmemoryHadoopConfiguration.BM_THREADS, false);
		conf.setInt(BigmemoryHadoopConfiguration.BM_COMMIT_THREADS, 10);
		TaskAttemptContext tac = new TaskAttemptContext(conf , new TaskAttemptID());
		
		BigmemoryOutputFormat bmof = new BigmemoryOutputFormat();
		try {
			OutputCommitter  oc = bmof.getOutputCommitter(tac);
			Assert.assertEquals(true, (oc instanceof BigmemoryOutputCommitter));
		} catch (IOException e) {
			LOG.info("IOException occured");
			Assert.fail("Failed to get an instance of BigMemoryOutputCommitter");
		} catch (InterruptedException e) {
			Assert.fail("Failed to get an instance of BigMemoryOutputCommitter");
		}
	}
  
}
