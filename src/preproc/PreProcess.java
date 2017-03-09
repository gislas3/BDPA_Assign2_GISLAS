package preproc;

//import all the necessary classes
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PreProcess extends Configured implements Tool {

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
		int status = ToolRunner.run(new PreProcess(), args); //start the job

		System.exit(status);
	}

	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job myjob = Job.getInstance(getConf()); 
		myjob.setJarByClass(PreProcess.class); 
		
		// set output key and value classes for reducer
		myjob.setOutputKeyClass(LongWritable.class); 
		myjob.setOutputValueClass(Text.class);
		
		//set output key and value classes for the mapper
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(LongWritable.class);

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
			 //code for printing the number of lines to a separate text file
			 FileSystem fs = FileSystem.get(getConf()); 
			 String output_path = "num_lines.txt"; //save the file as num_lines.txt
			 FSDataOutputStream out = fs.create(new Path(output_path));//will create a new file in the current directory called num_lines.txt
			 Counters counters = myjob.getCounters(); 
			 Long outputrec = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", 
			           "REDUCE_OUTPUT_RECORDS").getValue(); //get the default counter for the number of output records
			 //System.out.println("outputrec is " + outputrec);
			    			out.writeBytes(""+outputrec);// write that value to a file
			    	
			    out.close();	//close the stream
			return 0;
		} 
		//job was unsuccessful
		else {
			return 1;
		}
	}

	//overrides mapper class - outputs as key value pairs words, and value the line number
	public static class Map extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private HashSet<String> swords; //store the stop words 
		
		//setup will read in the stopwords.csv file, and store the words in the global HashSet swords
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			swords = new HashSet<String>(); // initialize swords
			String sw = fs.getHomeDirectory().toString() + "/stopwords.csv"; // assumes stopwords.csv file exists in system's home directory

			try {

				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(sw)))); // open the file
				try { 
					String line;

					line = br.readLine();

					while (line != null) { //read through the file, getting all the stop words
						String[] linesplit = line.split(",");
						swords.add(linesplit[0]);

						line = br.readLine();
					}
				} finally {

					br.close();
				}
			} catch (IOException e) {
				System.out.println(e.toString());
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().toLowerCase().split("\\W"); //split on nonword characters (defined in java as all non alphanumeric characters)
			for (int i = 0; i < line.length; i++) {
				if (!swords.contains(line[i]) && !line[i].isEmpty()) { //write to the context if non-empty and the word is not a stopword
					context.write(new Text(line[i]), key);
				}
			}

		}
	}

	//reducer takes in key value pairs of Text, LongWritable and outputs LongWritable, Text key value pairs
	//where the values to write are the document id and the line in the document
	public static class Reduce extends Reducer<Text, LongWritable, LongWritable, Text> {
		HashMap<String, Long> freqs = new HashMap<String, Long>(); //will store the frequencies of each word
		HashMap<Long, LinkedList<String>> docs = new HashMap<Long, LinkedList<String>>(); //will store the lines in main memory
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long fr = 0; //used to store the frequency
			LinkedList<Long> inds2 = new LinkedList<Long>(); //store all the document ids that the word appears in
			
			//calculate the frequency - since, iterator, have to store the values in another list to use again
			for (LongWritable ind : values) {
				fr++;
				inds2.add(ind.get());
			}
			freqs.put(key.toString(), fr); //add the word to the table storing the frequencies
			for (Long curr : inds2) {
				if (docs.get(curr) == null) { // initial condition, initializing the list of words in the document
					LinkedList<String> wordlist = new LinkedList<String>(); 
					wordlist.add(key.toString());
					docs.put(curr, wordlist); //add the list to the document
				} else {
					if (!docs.get(curr).contains(key.toString())) { //if the document doesn't already contain the word
						int index = 0;
						//loop through until find correct index to put word in 
						while (index < docs.get(curr).size() && fr > freqs.get(docs.get(curr).get(index))) {
							index++;
						}
						docs.get(curr).add(index, key.toString()); //add the word to the index
					}
				}
			}
		}

		//print out all the lines to the context
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Object[] keys  = docs.keySet().toArray(); //get the keys
			Arrays.sort(keys); //sort them so prints out in correct order in terms of line number - if this doesn't matter, can comment out for faster runtime
			for (int i = 0; i < keys.length; i++) {

				LinkedList<String> currdoc = docs.get(keys[i]);
				String toprint = "";
				for (int j = 0; j < currdoc.size(); j++) {
					toprint = toprint + currdoc.get(j) + "\t"; //build the string
				}

				context.write(new LongWritable(i+1), new Text(toprint.trim())); //write to the context
			}
		}
	}

}
