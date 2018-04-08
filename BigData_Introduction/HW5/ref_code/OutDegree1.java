/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.hadoop.examples;
 
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OutDegree1
{
	public static class myComparator extends Comparator {  
        @SuppressWarnings("rawtypes")  
        public int compare( WritableComparable a,WritableComparable b){  
            return 0; 
        }  
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
            byte[] _b1 = Arrays.copyOfRange(b1, s1 + 1, s1 + l1);
			byte[] _b2 = Arrays.copyOfRange(b2, s2 + 1, s2 + l2);
			String[] t1 = new String(_b1).split("\t");
			String[] t2 = new String(_b2).split("\t");
			int nA1 = 0;
			if(t1.length > 1){
				nA1 = Integer.parseInt(t1[1]);
			}

			int nA2 = Integer.parseInt(t1[0]);
			
			int nB1 = 0; 
			if(t2.length > 1){
                nB1 = Integer.parseInt(t2[1]);
			}
			
			int nB2 = Integer.parseInt(t2[0]);

			int compare1 = nA1 - nB1;
			int compare2 = nA2 - nB2;
			if(compare1 != 0){
				return -compare1;
			}else{
				if(compare2 != 0){
					return -compare2;
				}else{
					return 0;
				}
			}  
        }  
    } 

	public static class OutDegreeMapper1 
		extends Mapper<Object, Text, Text, IntWritable>
	{
    
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = value.toString();
			String[] subline = oneLine.split(" ");
			word.set(subline[1]);
			context.write(word,one);
		}
	}

	public static class OutDegreeMapper2 
		extends Mapper<Object, Text, Text, IntWritable>
	{
    
		private Text word = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = value.toString();
			String[] subline = oneLine.split("\t");
			word.set(subline[0]);
			context.write(new Text(word+"\t"+subline[1]),new IntWritable(Integer.valueOf(subline[1])));
		}
	}

	public static class OutDegreeReducer1 
		extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
  
	public static class OutDegreeReducer2 
		extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = key.toString();
			String[] subline = oneLine.split("\t");
			System.out.println(subline.length);
 			int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
			context.write(new Text(subline[0]), result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: OutDegree1 <in> [<in>...] <out>");
			System.exit(2);
		}
		Configuration job1conf = new Configuration();
		Job job1 = new Job(job1conf, "CountOutDegree");
		job1.setJarByClass(OutDegree1.class);
		job1.setMapperClass(OutDegreeMapper1.class);
		job1.setCombinerClass(OutDegreeReducer1.class);
		job1.setReducerClass(OutDegreeReducer1.class);
		job1.setOutputKeyClass(Text.class);
        job1.setNumReduceTasks(1);
		job1.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) 
		{
			FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job1,
				new Path(otherArgs[otherArgs.length - 1]));
		job1.waitForCompletion(true);


		Configuration job2conf = new Configuration();
		Job job2 = new Job(job2conf, "SortOutDegree");
		job2.setJarByClass(OutDegree1.class);
		job2.setMapperClass(OutDegreeMapper2.class);
		job2.setCombinerClass(OutDegreeReducer2.class);
		job2.setReducerClass(OutDegreeReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
		job2.setSortComparatorClass( myComparator.class); 
		FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 1]+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job2,
				new Path(otherArgs[otherArgs.length - 1]+"/final"));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

