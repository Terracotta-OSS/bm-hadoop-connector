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
