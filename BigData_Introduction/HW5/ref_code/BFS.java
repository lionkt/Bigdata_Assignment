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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

public class BFS {

	public static class Node{
		public enum COLOR{
			WHITE,
			GRAY,
			BLACK
		};

		private String ID = "NULL";
		private int Distance = Integer.MAX_VALUE;
		private String AdjInfo = "NULL";
		private COLOR Color = COLOR.WHITE;

		public Node(){
			Distance = Integer.MAX_VALUE;
			Color = COLOR.WHITE;
			AdjInfo = "NULL";
			ID = "NULL";
		}

		public Node(String NodeID,String NodeInfo){
			String[] subinfo = NodeInfo.split(" ");
			ID = NodeID;
			AdjInfo = subinfo[0];
			if(subinfo[1].equals("Integer.MAX_VALUE"))
				Distance = Integer.MAX_VALUE;
			else
				Distance = Integer.parseInt(subinfo[1]);
			Color = COLOR.valueOf(subinfo[2]);
		}

		public void setColor(COLOR color){
			this.Color = color;
		}

		public void setDistance(int distance){
			this.Distance = distance;
		}

		public void setID(String ID){
			this.ID = ID;
		}

		public void setAdjInfo(String Info){
			this.AdjInfo = Info;
		}


		public String getID(){
			return ID;
		}

		public String getAdjInfo(){
			return AdjInfo;
		}

		public COLOR getColor(){
				return Color;
		}

		public int getDistance(){
				return Distance;
		}

		public String getInfo(){
			return this.getAdjInfo() + " " + String.valueOf(this.Distance) + " " + String.valueOf(this.Color);
		}
	}

	public static class NodeMapper
			extends Mapper <Object,Text,Text,Text>
	{
		public void map(Object key, Text value,Context context
			) throws IOException, InterruptedException 
		{
			//Node mapNode = new Node(key.toString(),value.toString());
			//String oneLine = value.toString();
			//String[] subLine = oneLine.split("\t");
			context.write(new Text(key.toString()),new Text(value.toString()));
		}
	}

	//<String, String, String, Node>
	public static class BFSMapper 
			extends NodeMapper
	{
		public static enum NewNode{
			NewNodeCounter
		}
		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
		{
			//Node thisNode = new Node(key.toString(),value.toString());
			String oneLine = value.toString();
			String[] subLine = oneLine.split("\t");
			Node thisNode = new Node(subLine[0],subLine[1]);
			//context.write(new Text(key.toString()),new Text(value.toString()));
			if(thisNode.getColor().equals(Node.COLOR.GRAY)){
				//isFinishedFlag = false;
				thisNode.setColor(Node.COLOR.BLACK);
				context.write(new Text(thisNode.getID()),new Text(thisNode.getInfo()));
				String[] adjNode = thisNode.getAdjInfo().split(",");
				Configuration conf = context.getConfiguration();
				int thisDis = Integer.parseInt(conf.get("IterNum"));
				if(adjNode[0].equals("NULL"))
					return;
				for(int i = 0; i < adjNode.length; i++){
					Node temp = new Node();
					temp.setColor(Node.COLOR.GRAY);
					temp.setDistance(thisDis); 
					temp.setID(adjNode[i]);
					context.getCounter(NewNode.NewNodeCounter).increment(1);
					super.map(new Text(temp.getID()),new Text(temp.getInfo()),context);
				}
			} else {
				context.write(new Text(subLine[0]),new Text(subLine[1]));
			}
		}
	}
  
	public static class BFSReducer 
		extends Reducer<Text,Text,Text,Text>
	{
		protected void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException 
		{
			Node reduceNode = new Node();
			reduceNode.setID(key.toString());
			for (Text temp : values)
			{
				Node val = new Node(key.toString(),temp.toString());
				if(reduceNode.getColor().compareTo(val.getColor())<0)
					reduceNode.setColor(val.getColor());
				if(reduceNode.getDistance() > val.getDistance())
					reduceNode.setDistance(val.getDistance());
				if(reduceNode.getAdjInfo().equals("NULL") && !val.getAdjInfo().equals("NULL"))
					reduceNode.setAdjInfo(val.getAdjInfo());
			}
			context.write(key, new Text(reduceNode.getInfo()));
		}
	}

	public static class myComparator extends Comparator {  
        @SuppressWarnings("rawtypes")  
        public int compare( WritableComparable a,WritableComparable b){  
            return 0; 
        }  
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
            byte[] _b1 = Arrays.copyOfRange(b1, s1 + 1, s1 + l1);
			byte[] _b2 = Arrays.copyOfRange(b2, s2 + 1, s2 + l2);
			String[] t1 = new String(_b1).split(",");
			String[] t2 = new String(_b2).split(",");
			int nA1 = 0;
			int nA2 = Integer.parseInt(t1[0]);
			int nB1 = 0;
			int nB2 = Integer.parseInt(t2[0]);
			if(t1.length > 1){
				nA1 = Integer.parseInt(t1[1]);
				//nA2 = Integer.parseInt(t1[0]);
			}
			if(t2.length > 1){
                nB1 = Integer.parseInt(t2[1]);
                //nB2 = Integer.parseInt(t2[0]);
			}
			int compare1 = nA1 - nB1;
			int compare2 = nA2 - nB2;
			if(compare1 != 0){
				return compare1;
			}
			else{
				if(compare2 != 0){
					return -compare2;
				}else{
					return 0;
				}
			} 
			//else{return 0;} 
        }  
    }

    public static class FinalMapper 
		extends Mapper<Object, Text, Text, IntWritable>
	{
    
		private Text word = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = value.toString();
			String[] subline = oneLine.split("\t");
			String[] subinfo = subline[1].split(" ");
			if(subinfo[1].equals(String.valueOf(Integer.MAX_VALUE)))
				return;
			word.set(subline[0]+","+subinfo[1]);
			context.write(word,new IntWritable(Integer.parseInt(subinfo[1])));
		}
	}

	public static class FinalReducer
		extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();
		private Text word = new Text();
		
		public void reduce(Text key, Iterable<IntWritable> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			for (IntWritable temp : values)
			{
				String oneLine = key.toString();
				String[] subline = oneLine.split(",");
				word.set(subline[0]);
				context.write(word,temp);
			}
		}
	}

	//static boolean isFinishedFlag = false;

  public static void main(String[] args) throws Exception {
  	int IterNum = 1;
  	while(true){//isFinishedFlag == false &&
  		//isFinishedFlag = true;
  		Configuration conf = new Configuration();
  		conf.setStrings("IterNum",String.valueOf(IterNum));
  		Job job = new Job(conf,"BFS_"+String.valueOf(IterNum));
  		job.setJarByClass(BFS.class);
  		job.setMapperClass(BFSMapper.class);
  		job.setCombinerClass(BFSReducer.class);
  		job.setReducerClass(BFSReducer.class);
  		job.setOutputKeyClass(Text.class);
  		job.setOutputValueClass(Text.class);
        if(IterNum == 1)
        	FileInputFormat.addInputPath(job, new Path("./bfs/bfs-" + IterNum + "-out"));
        else
        	FileInputFormat.addInputPath(job, new Path("./bfs/bfs-" + IterNum + "-out"));
        FileOutputFormat.setOutputPath(job, new Path("./bfs/bfs-"+ (IterNum+1) +"-out"));

        job.waitForCompletion(true);
        if(job.getCounters().findCounter(BFSMapper.NewNode.NewNodeCounter).getValue() == 0)
        	break;
        IterNum++;
  	}
  	//int IterNum = 10;
  	Configuration job2conf = new Configuration();
	Job job2 = new Job(job2conf, "FinalJob");
	job2.setJarByClass(BFS.class);
	job2.setMapperClass(FinalMapper.class);
	job2.setCombinerClass(FinalReducer.class);
	job2.setReducerClass(FinalReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(IntWritable.class);
	job2.setSortComparatorClass( myComparator.class); 
	FileInputFormat.addInputPath(job2, new Path("./bfs/bfs-" + IterNum + "-out"));
	FileOutputFormat.setOutputPath(job2,
                new Path("./bfs/bfs-final-out"));
    job2.setNumReduceTasks(1);
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}

