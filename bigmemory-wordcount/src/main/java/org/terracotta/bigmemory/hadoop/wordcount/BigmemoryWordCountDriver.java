package org.terracotta.bigmemory.hadoop.wordcount;

import org.terracotta.bigmemory.hadoop.BigmemoryElementWritable;
import org.terracotta.bigmemory.hadoop.BigmemoryOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The app is the classic Hadoop word count with a twist--the final outputs (number of occurrences of a word)
 * are stored in Bigmemory Max in a cache chosen by the user.
 *
 */
public class BigmemoryWordCountDriver extends Configured implements Tool  {

  public static void main(String[] args) throws Exception {
    int exitCode  = ToolRunner.run(new BigmemoryWordCountDriver(), args);
    System.exit(exitCode);
  }

  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] BigmemoryWordCountDriver <input path> <output cache> ", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(-1);
    }

    Configuration configuration = getConf();
    configuration.set("bigmemory.output.cache", args[1]);

    Job job = new Job(configuration, "Bigmemory Word count");
    job.setJarByClass(getClass());
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(BigmemoryOutputFormat.class);
    job.setReducerClass(BigmemoryHadoopReducer.class);
    job.setOutputValueClass(BigmemoryElementWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));

    job.setMapperClass(BigmemoryTokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }


}


