import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper extends Mapper<LongWritable, Text, Text, LongPairWritable> {
		private final static LongPairWritable one = new LongPairWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			JSONObject record = new JSONObject(value.toString());
			String subreddit = record.getString("subreddit");
			long score = record.getLong("score");

			one.set(1L, score);
			context.write(new Text(subreddit), one);

		}
	}

	public static class RedditCombiner extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable result = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {

			long count = 0;
			long score = 0;

			for (LongPairWritable pair : values) {
				count += pair.get_0();
				score += pair.get_1();
			}

			result.set(count, score);
			context.write(key, result);

		}
	}

	public static class RedditReducer extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {

			long count = 0;
			long score = 0;

			for (LongPairWritable pair : values) {
				count += pair.get_0();
				score += pair.get_1();
			}

			double average = ((double)score / (double)count);
			result.set(average);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "RedditAverage");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setCombinerClass(RedditCombiner.class);
		job.setReducerClass(RedditReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
