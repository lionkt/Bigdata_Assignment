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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;

public class FindAdjecent {

	public static class TokenizerMapper 
			extends Mapper<Object, Text, Text, Text>
	{
    
        private Text word = new Text();
        private Text son = new Text();
      
		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
		{

			String oneLine = value.toString();
			String[] subline = oneLine.split(" ");
            word.set(subline[1]);
            son.set(subline[2]);
			context.write(word,son);
			
		}
	}
  
	public static class IntSumReducer 
		extends Reducer<Text,Text,Text,Text> 
	{
		// private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException 
		{
            String temp = "";
			for (Text val : values) 
			{
				temp += val.toString() + " ";
            }
            String refine_temp = removesamestring(temp);
			context.write(key, new Text(refine_temp));
		}
    }
    

    private static String removesamestring(String str)   
    {
        Set<String> mlinkedset = new LinkedHashSet<String>();
        String[] strarray = str.split(" ");
        StringBuffer  sbf = new StringBuffer ();
        for (int i = 0; i < strarray.length; i++) {
            if (!mlinkedset.contains(strarray[i])) {
                mlinkedset.add(strarray[i]);
                sbf.append(strarray[i] + " ");
            }
        }
        return sbf.toString().substring(0, sbf.toString().length() - 1);
    }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Find adjecent");
    job.setJarByClass(FindAdjecent.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);			// set output number
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

