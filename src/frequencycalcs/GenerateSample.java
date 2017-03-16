package frequencycalcs;

//import all the necessary classes
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenerateSample extends Configured implements Tool {

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
		int status = ToolRunner.run(new GenerateSample(), args); //start the job

		System.exit(status);
	}

	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job myjob = Job.getInstance(getConf()); 
		myjob.setJarByClass(GenerateSample.class); 
		
		// set output key and value classes for reducer
		myjob.setOutputKeyClass(LongWritable.class); 
		myjob.setOutputValueClass(Text.class);
		
		//set output key and value classes for the mapper
		myjob.setMapOutputKeyClass(LongWritable.class);
		myjob.setMapOutputValueClass(Text.class);

		myjob.setMapperClass(Map.class); //set the mapper class
		
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
			Mapper<LongWritable, Text, LongWritable, Text> {
		private long num_records = 0;
		private HashSet<Long> docstouse = new HashSet<Long>();
		private int sample_size = 5000;
		private int seed = 1997;
		private Random rand = new Random(seed);
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			int count = 0;
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String sw = fs.getHomeDirectory().toString() + "/num_lines.txt"; 
			try {

				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(sw)))); // open the file
				try { 
					String line;

					line = br.readLine();
					num_records = Long.parseLong(line); // store the number of records to use as the denominator
				} finally {

					br.close();
				}
			} catch (IOException e) {
				System.out.println(e.toString());
			}
			//System.out.println(num_records);
			while(count < sample_size) {
				long toadd = Math.abs(rand.nextLong())%num_records + 1;
				//System.out.println(toadd);
				if(!docstouse.contains(toadd)) {
					docstouse.add(toadd);
					count++;
				}
			}
			
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s"); //split on nonword characters (defined in java as all non alphanumeric characters)
			long candidate = Long.parseLong(line[0]);
			if(docstouse.contains(candidate)) {
				context.write(new LongWritable(candidate), new Text(value.toString().substring(line[0].length() + 1))); // 
			}
		}
	}

}
