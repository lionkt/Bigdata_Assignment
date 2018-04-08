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


public class Task3_BFS {

    // node class refer to http://irwenqiang.iteye.com/blog/1541559
    public static class TreeNode {
        public enum COLOR {
            WHITE, GRAY, BLACK
        }
        private String m_id;
        private int m_distance;
        private String m_adj;                  // 子节点信息
        private COLOR m_color;

        public TreeNode() {
            this.m_id = "NULL";
            this.m_distance = Integer.MAX_VALUE;
            this.m_adj = "NULL";
            this.m_color = COLOR.WHITE;
        }

        public TreeNode(String id, String info) {
            // info的格式为 1 2 3 X|distance|color
            String[] temp = info.split("\\|");
            this.m_id = id;
            this.m_adj = temp[0];
            this.m_color = COLOR.valueOf(temp[2]);
            if (temp[1].equals("Integer.MAX_VALUE"))
                this.m_distance = Integer.MAX_VALUE;
            else
                this.m_distance = Integer.parseInt(temp[1]);
        }

        public String getId() {
            return this.m_id;
        }

        public int getDistance() {
            return this.m_distance;
        }

        public String getAdj() {
            return this.m_adj;
        }

        public COLOR getColor() {
            return this.m_color;
        }

        public void setId(String id) {
            this.m_id = id;
        }

        public void setDistance(int distance) {
            this.m_distance = distance;
        }

        public void setAdj(String adjInfo) {
            this.m_adj = adjInfo;
        }

        public void setColor(COLOR color) {
            this.m_color = color;
        }

        public String getInfo(){
            StringBuffer str = new StringBuffer();
            str.append(m_adj);
            str.append("|");
            str.append(String.valueOf(this.m_distance));
            str.append("|");
            str.append(this.m_color);
            return str.toString();
        }

    }

    public static class BFSMapper extends Mapper<Object, Text, Text, Text> //<Text, Text, Text, Node>
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
                context.write(new Text(thisNode.getId()), new Text(thisNode.getInfo()));
                String[] adjNode = thisNode.getAdj().split(" ");
                Configuration conf = context.getConfiguration();
                int thisDis = Integer.parseInt(conf.get("Iter_Num"));
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
                    context.write(new Text(temp.getId()), new Text(temp.getInfo()));
//                    super.map(new Text(temp.getID()), new Text(temp.getAllInfo()), context);
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
            TreeNode.COLOR color = TreeNode.COLOR.WHITE;
            String adjInfo = "NULL";
            int distance = Integer.MAX_VALUE;

            for (Text temp : values) {
                TreeNode temp_val = new TreeNode(key.toString(), temp.toString());
                // 选择最深的颜色
                if (targetNode.getColor().compareTo(temp_val.getColor()) < 0)
                    color = temp_val.getColor();
                // 选择最近的距离
                if (targetNode.getDistance() > temp_val.getDistance())
                    distance = temp_val.getDistance();
                // 归并邻接点信息
                if (targetNode.getAdj().equals("NULL") && !temp_val.getAdj().equals("NULL"))
                    adjInfo = temp_val.getAdj();
            }

            targetNode.setId(key.toString());
            targetNode.setColor(color);
            targetNode.setDistance(distance);
            targetNode.setAdj(adjInfo);

            context.write(key, new Text(targetNode.getInfo()));
        }
    }

    // 对所有点的信息再进行一次汇总，提取出距离信息
    public static class BFSMapper2 extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException
        {
            // info的格式为 1 2 3 X|distance|color
            String oneLine = value.toString();
            String [] subLine = oneLine.split("\t");
            String [] subInfo = subLine[1].split("\\|");
            if (subInfo[1].equals(String.valueOf(Integer.MAX_VALUE)))
                return;
            context.write(new Text(subLine[0]), new IntWritable(Integer.parseInt(subInfo[1])));

        }
    }
    public static class BFSReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable temp : values) {
//                String oneLine = key.toString();
//                String[] subline = oneLine.split(" ");
//                word.set(subline[0]);
                context.write(key, temp);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int Iter_Num = 1;
        while (true) {
            Configuration conf = new Configuration();
            conf.setStrings("Iter_Num", String.valueOf(Iter_Num));
            Job job = new Job(conf, "BFS_" + String.valueOf(Iter_Num));
            job.setJarByClass(Task3_BFS.class);
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
        job2.setJarByClass(Task3_BFS.class);
        job2.setMapperClass(BFSMapper2.class);
        job2.setReducerClass(BFSReducer2.class);
        job2.setCombinerClass(BFSReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("./BFS-caseN/BFS-" + Iter_Num + "-out"));
        FileOutputFormat.setOutputPath(job2, new Path("./BFS-caseN/BFS-FINAL-out"));
        job2.setNumReduceTasks(1);         // 最后只输出一个文件，方便观察数据
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }


}
