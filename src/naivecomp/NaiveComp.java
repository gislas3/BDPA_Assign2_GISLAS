package naivecomp;

//import all the necessary classes
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class NaiveComp extends Configured implements Tool {

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
		int status = ToolRunner.run(new NaiveComp(), args); //start the job

		System.exit(status);
	}
	enum Comparisons{ TOTAL //define the counter to be used to track comparisons
		}
	@Override
	public int run(String args[]) throws Exception {
		//initialize the job
		Job myjob = Job.getInstance(getConf()); 
		myjob.setJarByClass(NaiveComp.class); 
		
		// set output key and value classes for reducer
		myjob.setOutputKeyClass(Text.class); 
		myjob.setOutputValueClass(DoubleWritable.class);
		
		//set output key and value classes for the mapper
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(Text.class);

		myjob.setMapperClass(Map.class); //set the mapper class
		myjob.setNumReduceTasks(10); //use one reducer
		myjob.setReducerClass(Reduce.class); //set the reducer class
		myjob.setPartitionerClass(Partition.class); //set the partitioner class (necessary if use multiple reducers)
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
		private LinkedList<String> doc_ids = new LinkedList<String>(); //store the document ids
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s"); //split on nonword characters (defined in java as all non alphanumeric characters)
			String d_id = line[0]; // get the document id (id documentid 2)
			Text doc_val = new Text(value.toString().substring(line[0].length()).trim()); //get the value of the document (get rid of the document_id/key)
			context.write(new Text(d_id), new Text(doc_val)); //write the document to the context
			for(int i = 0; i < doc_ids.size(); i++) { //write all the document ids seen so far + the content of the current document
				Text keytext = new Text(doc_ids.get(i) + "#" + d_id);
				context.write(keytext, doc_val);
			}
			doc_ids.add(d_id); //add the current document id to the list of document ids seen so far
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
					dline = dline + val.toString().trim(); // should just run once (get the content of the second document)
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
