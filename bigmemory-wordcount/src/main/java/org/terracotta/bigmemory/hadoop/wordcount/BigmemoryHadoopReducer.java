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
package org.terracotta.bigmemory.hadoop.wordcount;

import org.terracotta.bigmemory.hadoop.BigmemoryElementWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BigmemoryHadoopReducer extends Reducer<Text, IntWritable, Text, BigmemoryElementWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    BigmemoryElementWritable elementWritable = new BigmemoryElementWritable(key.toString(), sum);
    context.write(key, elementWritable);
  }
}
