package preproc;

//import all the necessary classes
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
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
		//myjob.setOutputKeyClass(LongWritable.class); 
		myjob.setOutputKeyClass(LongWritable.class);
		myjob.setOutputValueClass(Text.class);
		
		//set output key and value classes for the mapper
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(Text.class);
		//myjob.setMapOutputValueClass(LongWritable.class);

		myjob.setMapperClass(Map.class); //set the mapper class
		myjob.setNumReduceTasks(1); //use one reducer
		myjob.setCombinerClass(Combine.class);
		//myjob.setPartitionerClass(Partition.class);
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

	//overrides mapper class - outputs two separate types of key/value pairs
	// first type is word/frequency, the second type is of the form document_id/word
	//keys are output so as the former are output prior to the latter so frequencies can be
	//computed in the reducer prior to printing out the lines in ascending order of global frequency
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private HashSet<String> swords; //store the stop words 
		private Text word = new Text(); //used to store the word to be printed
		private String docid_template = "########################"; //so sorting works correctly on numbers in string format
		private final static Text ONE = new Text("1"); // for the frequency
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
					String oldkey = "" + key; // convert the key to a string
					String newkey = docid_template.substring(0, docid_template.length() - oldkey.length()) + oldkey; //format the key
					word.set(line[i]);
					context.write(word, ONE); // write the word and a count for its frequency
					context.write(new Text("{" + newkey), word); //write the doc id and the word
				}
			}

		}
	}

	//combiner class adds frequencies and concatenates strings (
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String key2 = key.toString();
			String val_towrite = "";
			long freq = 0;
			for(Text val : values) {
				if(key2.charAt(0) == '{') { //documentid key
					val_towrite  = val_towrite + val.toString() + "\t"; 
				}
				else { //word key
					freq++;
					val_towrite = "" + freq;
				}
			}
			context.write(key, new Text(val_towrite));
		}
	}
	
	/*
	//implement a partitioner in case want to use multiple reducers - that way can make sure 
	//all words in a document get sent to the same reducer; either have to figure out documents with
	// for disjoint sets of words, or send words multiple times across reducers; not implemented
	 // since only using one mapper and reducer
	public static class Partition extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numReduceTasks) {
        	if(numReduceTasks == 0) {
        		return 0;
        	}
        	Long newkey = Long.parseLong(key.toString().split("#")[0]); //get the document_id, and use that as the hashkey
        	return (newkey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
     }
	*/
	//reducer takes in key value pairs of Text, Text and outputs LongWritable, Text key value pairs
	//where the values to write are the document id and the line in the document
	public static class Reduce extends Reducer<Text, Text, LongWritable, Text> {
		private HashMap<String, Integer> freqs = new HashMap<String, Integer>(); //will store the frequencies of each word
		private long counter = 0; //keep track of number of documents printed (only works if using one reducer)
		
		//assumes words are sorted prior to document ids
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int freq = 0; //computes frequency of the word
			LinkedList<String> linewords = new LinkedList<String>(); //stores the words for the current line
			for(Text val : values) {
				if(key.charAt(0) != '{') { //word key
					freq += Integer.parseInt(val.toString());
				} 
				else { //document id key
					String[] valwords = val.toString().split("\\s");
					for(int i = 0; i < valwords.length; i++) {
						int index = 0;
						String theword = valwords[i];
						if(!linewords.contains(theword)) { //only add to list if unique qord
							int currfreq = freqs.get(theword);
							while(index < linewords.size() && currfreq > freqs.get(linewords.get(index))) { //find its position in the list
								index++;
							}
							linewords.add(index, theword); //add the word to the list
						}
					}
				}
			}
			if(key.charAt(0) != '{') { //add the word and frequency
				freqs.put(key.toString(), freq);
			}
			else {
				String thekey = key.toString();
				int begind = thekey.lastIndexOf("#") + 1;
				long docid = Long.parseLong(thekey.substring(begind));
				String finaloutput = "";
				for(String s : linewords) {
					finaloutput = finaloutput + s + "\t"; //build the string 
				}
				if(counter == 0) { //start from 1 if at beginning of document - else, start from offset
					counter = docid + 1;
				}
				context.write(new LongWritable(counter), new Text(finaloutput)); //write to the context
				counter++; //increment the counter
			}
		}
	}

}
