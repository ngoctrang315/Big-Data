package de.tud.stg;

import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
public class Comprehension
{
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text file = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		file.set(fileName);
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
		 context.write(word, file);
		 }
		 }
		 }
		
		 public static class InvertedReducer extends Reducer<Text, Text, Text, Text> {
		 private Text result = new Text();
		
		 @Override
		 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		 StringBuilder sb = new StringBuilder();
		 for (Text val: values) {
		 sb.append(val);
		
		 if(values.iterator().hasNext()) {
		 sb.append(",");
		 }
		 }
		 result.set(sb.toString());
		 context.write(key, result);
		 }
		 }
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Comprehension");
        job.setJarByClass(Comprehension.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvertedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
