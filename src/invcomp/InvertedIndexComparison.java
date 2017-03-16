package invcomp;

//import all the necessary classes
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexComparison extends Configured implements Tool {

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
		int status = ToolRunner.run(new InvertedIndexComparison(), args); //start the job

		System.exit(status);
	}
	enum Comparisons{ TOTAL
		}
	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job myjob = Job.getInstance(getConf()); 
		myjob.setJarByClass(InvertedIndexComparison.class); 
		
		// set output key and value classes for reducer
		myjob.setOutputKeyClass(Text.class); 
		myjob.setOutputValueClass(DoubleWritable.class);
		
		//set output key and value classes for the mapper
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(Text.class);

		myjob.setMapperClass(Map.class); //set the mapper class
		myjob.setNumReduceTasks(10); //use one reducer
		myjob.setReducerClass(Reduce.class); //set the reducer class
		myjob.setPartitionerClass(Partition.class);
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
		private HashMap<String, HashSet<String>> pairs = new HashMap<String, HashSet<String>>(); //boolean matrix that stores pairs to output
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			//change the following line to "/inverted_index_sample.txt" for running on the sample output
			String sw = fs.getHomeDirectory().toString() + "/inverted_index.txt"; // assumes inverted_index.txt file exists in system's home directory

			try {

				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(sw)))); // open the file
				try { 
					String line;

					line = br.readLine();

					while (line != null) { //read through the file, write pairs to the hashmap
						String[] linesplit = line.split("\\s");
						if(linesplit.length > 2) { //only loop through line if has multiple documents with that word in it
							for(int i = 1; i < linesplit.length; i++) {
								String key1 = linesplit[i]; //the key of the first document
								for(int j = i+1; j < linesplit.length; j++) { 
									String key2 = linesplit[j]; //key of the second document
									String putkey = key1;
									String valkey = key2;
									if(Long.parseLong(key1) < Long.parseLong(key2)) { //do this since storing only lower triangular portion of matrix
										putkey = key2;
										valkey = key1;										
									}
									if(pairs.get(putkey) == null) { //initialize the hashset at putkey
										HashSet<String> temp = new HashSet<String>();
										temp.add(valkey);
										pairs.put(putkey, temp);
									}
									else {
										pairs.get(putkey).add(valkey); //add the other key 
									}

								}
							}
						}
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
			String[] line = value.toString().split("\\s"); //split on nonword characters (defined in java as all non alphanumeric characters)
			String d_id = line[0]; // get the document id (id documentid 2)
			Text doc_val = new Text(value.toString().substring(line[0].length())); //get the value of the document (get rid of the document_id/key)
			context.write(new Text(d_id), new Text(doc_val)); //write the document to the context
			HashSet<String> temp = pairs.get(d_id);
			if(temp != null) {
				for(String s : temp ) {
					context.write(new Text(s + "#" + d_id), new Text(doc_val)); //only write the candidates to the context
				}
			}
		}
	}
	
	//override partitioner so can use multiple reducers
	public static class Partition extends Partitioner<Text, Text> {
		@Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
			if(numReduceTasks == 0) {
				return 0;
			}
			String key_part = key.toString().split("#")[0]; //want to partition by first document id to make sure can compare documents across reducers
			return (key_part.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	//reducer takes in key value pairs of Text, LongWritable and outputs LongWritable, Text key value pairs
	//where the values to write are the document id and the line in the document
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		private String[] current_doc; //used to store the content of the first documentid
		private double thresh = .8; //threshold for similarity
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				String dline = "";
				for(Text val: values) {
					dline = dline + val.toString(); // should just run once (get the content of the second document)
				}
				if(!key.toString().contains("#")) { //store document content if looking at new document
					current_doc = dline.split("\\s");
				}
				else {
					context.getCounter(Comparisons.TOTAL).increment(1); //increment the counter tracking number of comparisons
					String[] comp_doc = dline.split("\\s"); //store the value of the current document to compare
					double num = 0;
					for(int i = 0; i < current_doc.length; i++) {
						String s1 = current_doc[i];
						for(int j = 0; j < comp_doc.length; j++) { //compare the two documents
							String s2 = comp_doc[j];
							if(s1.equals(s2)) { 
								num++; //number of matching words (D1 intersect D2)
							}
						}
					}
					double denom = comp_doc.length + current_doc.length - num; // D1 union D2
					double sim = num/denom; //similarity score
					if(Double.compare(num/denom, thresh) >=0 ) {
						context.write(key, new DoubleWritable(sim)); //write to the context if the similarity is larger than the threshold
					}
				}
		}

		
	}

}
