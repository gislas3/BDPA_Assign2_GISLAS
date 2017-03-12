package invcomp;

//import all the necessary classes
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class InvertedIndex extends Configured implements Tool {

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
		int status = ToolRunner.run(new InvertedIndex(), args); //start the job

		System.exit(status);
	}

	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job myjob = Job.getInstance(getConf()); 
		myjob.setJarByClass(InvertedIndex.class); 
		
		// set output key and value classes for reducer
		myjob.setOutputKeyClass(Text.class); 
		myjob.setOutputValueClass(Text.class);
		
		//set output key and value classes for the mapper
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(Text.class);

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
			Mapper<LongWritable, Text, Text, Text> {
		private double thresh = .8;
		Text docid_write = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s"); //split on nonword characters (defined in java as all non alphanumeric characters)
			String d_id = line[0]; // get the document id
			int numtowrite = (int) Math.round((line.length - 1) - thresh * (line.length -1) + 1); //number of words to use for the inverted index
			docid_write.set(d_id);
			for(int i = 1; i <= numtowrite; i++) { //write the words to the context
				context.write(new Text(line[i]), docid_write );// write the word and the doc_id to the context
			}
		}
	}
	

	//reducer takes in key value pairs of Text, LongWritable and outputs LongWritable, Text key value pairs
	//where the values to write are the document id and the line in the document
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String finaloutput = "";
			for (Text val : values) {
			finaloutput = finaloutput + val.toString() + "\t"; // contatenate all the file names
		}
			context.write(key, new Text(finaloutput));
		}

		
	}

}
