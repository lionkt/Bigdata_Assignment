import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OutDegree {
	public static final int K = 20;

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



	public static class OutDegreeMapper2 extends Mapper<Object, Text, NullWritable, Text> {
		
		private Text word = new Text();
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String oneLine = value.toString();
			String[] subline = oneLine.split("\t");
			repToRecordMap.put(Integer.parseInt(subline[1]), new Text(value));

			if (repToRecordMap.size() > K) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static class OutDegreeReducer2 extends Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String oneLine = value.toString();
				String[] subline = oneLine.split("\t");

				repToRecordMap.put(Integer.parseInt(subline[1]), new Text(value));

				if (repToRecordMap.size() > K) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: OutDegree1 <in> [<in>...] <out>");
			System.exit(2);
		}
		Configuration job1conf = new Configuration();
		Job job1 = new Job(job1conf, "CountOutDegree");
		job1.setJarByClass(OutDegree.class);
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
		Job job2 = new Job(job2conf, "Top Ten Users by Reputation");
		job2.setJarByClass(OutDegree.class);
		job2.setMapperClass(OutDegreeMapper2.class);
		job2.setReducerClass(OutDegreeReducer2.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
        // job2.setSortComparatorClass(myComparator.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 1]+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]+"/Sort_output"));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
