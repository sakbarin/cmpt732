import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WikipediaPopular extends Configured implements Tool {

	public static class WikipediaMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private final static LongWritable result = new LongWritable();
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String input_line = value.toString();
			String[] line_values = input_line.split(" ");

			String visit_date = line_values[0];
			String visit_lang = line_values[1];
			String visit_page = line_values[2];
			long visit_count = Long.parseLong(line_values[3]);

			if (visit_lang.toLowerCase().equals("en") && !visit_page.equals("Main_Page") && !visit_page.startsWith("Special:")) {
				result.set(visit_count);
				word.set(visit_date);

				context.write(word, result);
			}
		}
	}

	public static class WikipediaReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

			long maximum_value = 0;

			for (LongWritable value : values) {
			
				if (value.get() > maximum_value)
					maximum_value = value.get();

			}

			result.set(maximum_value);
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "WikipediaPopular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikipediaMapper.class);
		job.setCombinerClass(WikipediaReducer.class);
		job.setReducerClass(WikipediaReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
