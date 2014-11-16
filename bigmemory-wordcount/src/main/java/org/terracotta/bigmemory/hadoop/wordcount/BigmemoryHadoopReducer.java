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
