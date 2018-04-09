
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

public class Frontier_BFS {

    // TreeNode class refer to http://irwenqiang.iteye.com/blog/1541559
    public static class TreeNode {
        public enum COLOR {
            WHITE, GRAY, BLACK
        };

        private String m_id;
        private int m_distance;
        private String m_adjInfo;
        private COLOR m_color;

        public TreeNode() {
            m_distance = Integer.MAX_VALUE;
            m_color = COLOR.WHITE;
            m_adjInfo = "NULL";
            m_id = "NULL";
        }

        public TreeNode(String NodeID, String NodeInfo) {
            String[] subinfo = NodeInfo.split("\\|");
            m_id = NodeID;
            m_adjInfo = subinfo[0];
            if (subinfo[1].equals("Integer.MAX_VALUE"))
                m_distance = Integer.MAX_VALUE;
            else
                m_distance = Integer.parseInt(subinfo[1]);
            m_color = COLOR.valueOf(subinfo[2]);
        }

        public void setColor(COLOR color) {
            this.m_color = color;
        }

        public void setDistance(int distance) {
            this.m_distance = distance;
        }

        public void setId(String id) {
            this.m_id = id;
        }

        public void setAdjInfo(String Info) {
            this.m_adjInfo = Info;
        }

        public String getId() {
            return m_id;
        }

        public String getAdjInfo() {
            return m_adjInfo;
        }

        public COLOR getColor() {
            return m_color;
        }

        public int getDistance() {
            return m_distance;
        }

        public String getAllInfo() {
            return this.getAdjInfo() + "|" + String.valueOf(this.m_distance) + "|" + String.valueOf(this.m_color);
        }
    }


    public static class BFSMapper extends Mapper<Object, Text, Text, Text>
    {
        public static enum NewNode {
            NewNodeCounter
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String oneLine = value.toString();
            String[] subLine = oneLine.split("\t");
            TreeNode thisNode = new TreeNode(subLine[0], subLine[1]);
            // 如果是灰色的，则进行bfs
            if (thisNode.getColor().equals(TreeNode.COLOR.GRAY)) {
                thisNode.setColor(TreeNode.COLOR.BLACK);
                context.write(new Text(thisNode.getId()), new Text(thisNode.getAllInfo()));
                String[] adjNode = thisNode.getAdjInfo().split(" ");
                Configuration conf = context.getConfiguration();
                int thisDis = thisNode.getDistance() + 1;
                // int thisDis = Integer.parseInt(conf.get("Iter_Num"));
                if (adjNode[0].equals("NULL")){
                    return;
                }
                // 将子节点的距离+1
                for (int i = 0; i < adjNode.length; i++) {
                    TreeNode temp = new TreeNode();
                    temp.setColor(TreeNode.COLOR.GRAY);
                    temp.setDistance(thisDis);
                    temp.setId(adjNode[i]);
                    context.getCounter(NewNode.NewNodeCounter).increment(1);
                    context.write(new Text(temp.getId()), new Text(temp.getAllInfo()));
                }
            } else {
                context.write(new Text(subLine[0]), new Text(subLine[1]));
            }
        }
    }

    public static class BFSReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeNode targetNode = new TreeNode();
            targetNode.setId(key.toString());
            for (Text temp : values) {
                TreeNode val = new TreeNode(key.toString(), temp.toString());
                // 选择最深的颜色
                if (targetNode.getColor().compareTo(val.getColor()) < 0)
                    targetNode.setColor(val.getColor());
                // 选择最近的距离
                if (targetNode.getDistance() > val.getDistance())
                    targetNode.setDistance(val.getDistance());
                // 归并邻接点信息
                if (targetNode.getAdjInfo().equals("NULL") && !val.getAdjInfo().equals("NULL"))
                    targetNode.setAdjInfo(val.getAdjInfo());
            }
            context.write(key, new Text(targetNode.getAllInfo()));
        }
    }

    // 对所有点的信息再进行一次汇总，提取出距离信息
    public static class BFSMapper2 extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String oneLine = value.toString();
            String[] subline = oneLine.split("\t");
            String[] subinfo = subline[1].split("\\|");
            if (subinfo[1].equals(String.valueOf(Integer.MAX_VALUE)))
                return;
            context.write(new Text(subline[0]), new IntWritable(Integer.parseInt(subinfo[1])));
        }
    }

    public static class BFSReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text word = new Text();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable temp : values) {
                String oneLine = key.toString();
                String[] subline = oneLine.split(" ");
                word.set(subline[0]);
                context.write(word, temp);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        int Iter_Num = 1;
        while (true) {
            Configuration conf = new Configuration();
            // conf.setStrings("Iter_Num", String.valueOf(Iter_Num));
            Job job = new Job(conf, "BFS_" + String.valueOf(Iter_Num));
            job.setJarByClass(Frontier_BFS.class);
            job.setMapperClass(BFSMapper.class);
            job.setCombinerClass(BFSReducer.class);
            job.setReducerClass(BFSReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            if (Iter_Num == 1)
                FileInputFormat.addInputPath(job, new Path("./BFS-caseN/BFS-" + Iter_Num + "-out"));
            else
                FileInputFormat.addInputPath(job, new Path("./BFS-caseN/BFS-" + Iter_Num + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("./BFS-caseN/BFS-" + (Iter_Num + 1) + "-out"));

            job.waitForCompletion(true);
            if (job.getCounters().findCounter(BFSMapper.NewNode.NewNodeCounter).getValue() == 0)
                break;
            Iter_Num++;
        }


        Configuration job2conf = new Configuration();
        Job job2 = new Job(job2conf, "Final");
        job2.setJarByClass(Frontier_BFS.class);
        job2.setMapperClass(BFSMapper2.class);
        job2.setReducerClass(BFSReducer2.class);
        job2.setCombinerClass(BFSReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("./BFS-caseN/BFS-" + Iter_Num + "-out"));
        FileOutputFormat.setOutputPath(job2, new Path("./BFS-caseN/BFS-FINAL-out"));
        job2.setNumReduceTasks(1);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
