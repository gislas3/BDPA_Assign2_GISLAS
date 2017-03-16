package frequencycalcs;

//import all the necessary classes
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvIndexFreq extends Configured implements Tool {

	//configure project setup
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("yarn-default.xml");
		Configuration.addDefaultResource("yarn-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	public static void main(String[] args) throws Exception {
		// basic check for proper input
		if (args.length != 2) {
			System.out.println("Usage: filename output_directory");
			System.exit(1);
		}
		int status = ToolRunner.run(new InvIndexFreq(), args); //start the job

		System.exit(status);
	}

	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job myjob = Job.getInstance(getConf()); 
		myjob.setJarByClass(InvIndexFreq.class); 
		
		// set output key and value classes for reducer
		myjob.setOutputKeyClass(IntWritable.class); 
		myjob.setOutputValueClass(DoubleWritable.class);
		
		//set output key and value classes for the mapper
		myjob.setMapOutputKeyClass(IntWritable.class);
		myjob.setMapOutputValueClass(IntWritable.class);

		myjob.setMapperClass(Map.class); //set the mapper class
		myjob.setNumReduceTasks(1); //use one reducer
		myjob.setReducerClass(Reduce.class); //set the reducer class
		
		//set input and output format classes to TextInput and TextOutput 
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);

		//get input and output directories from user of the program
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[1]));

		//run the job
		boolean to_ret = myjob.waitForCompletion(true);

		//job was successful
		if (to_ret) {
			return 0;
		} 
		//job was unsuccessful
		else {
			return 1;
		}
	}

	//overrides mapper class - outputs as key value pairs document id1, and content of documentid 2
	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		//private double thresh = .8;
		//Text docid_write = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s"); //split on nonword characters (defined in java as all non alphanumeric characters)
			context.write(new IntWritable(line.length-1), new IntWritable(1)); // 
		}
	}
	

	//reducer takes in key value pairs of Text, LongWritable and outputs LongWritable, Text key value pairs
	//where the values to write are the document id and the line in the document
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
		private Double num_records = 0.0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String sw = fs.getHomeDirectory().toString() + "/num_lines_sample.txt"; 
			try {

				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(sw)))); // open the file
				try { 
					String line;

					line = br.readLine();
					num_records = Double.parseDouble(line); // store the number of records to use as the denominator
				} finally {

					br.close();
				}
			} catch (IOException e) {
				System.out.println(e.toString());
			}
		}
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		double towrite = 0;
			for ( IntWritable val : values) {
			towrite += val.get();
		}

			context.write(key, new DoubleWritable(towrite/num_records));
		}

		
	}

}
