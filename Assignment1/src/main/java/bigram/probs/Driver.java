package bigram.probs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counters;

public class Driver extends Configured implements Tool{
	public enum COUNTERS {		//global counter to keep track of total number of bigrams
		  BIGRAMCOUNT
		 }

	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		//intermediate output files
		String countJobOut = "count-job-out";
		String probJobOut = "prob-job-out";

		/////////// count number of each bigram, global counter for total number of bigrams ///////////
        Job countJob = Job.getInstance(getConf());
        countJob.setJobName("BigramCount");
        countJob.setJarByClass(Driver.class);

        countJob.setMapperClass(CountMapper.class);
        countJob.setCombinerClass(CountReducer.class);
        countJob.setReducerClass(CountReducer.class);

        countJob.setOutputKeyClass(Bigram.class);
        countJob.setOutputValueClass(LongWritable.class);

        //if countOutPath already exists, remove existing file
        Path countOutPath = new Path(countJobOut);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(countOutPath)) {
			fs.delete(countOutPath, true);
		}

        FileInputFormat.addInputPath(countJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(countJob, countOutPath);

        countJob.waitForCompletion(true);

        /////////// mapper only job, calculate probability of each bigram based on total bigram count ///////////
        Counters counters = countJob.getCounters();
        Long counter = counters.findCounter(COUNTERS.BIGRAMCOUNT).getValue();	//save counter from countJob

        Job probJob = Job.getInstance(getConf());
        probJob.setJobName("BigramProb");
        probJob.setJarByClass(Driver.class);
        probJob.getConfiguration().setLong(Driver.COUNTERS.BIGRAMCOUNT.name(), counter);	//read in global bigram counter for use in mapper

        probJob.setMapperClass(ProbMapper.class);
        probJob.setNumReduceTasks(0);

        probJob.setOutputKeyClass(Bigram.class);
        probJob.setOutputValueClass(FloatWritable.class);

        //if probOutPath already exists, remove existing file
        Path probOutPath = new Path(probJobOut);
		if (fs.exists(probOutPath)) {
			fs.delete(probOutPath, true);
		}

        FileInputFormat.addInputPath(probJob, countOutPath);
        FileOutputFormat.setOutputPath(probJob, probOutPath);

        probJob.waitForCompletion(true);

        /////////// find most likely bigram starting with the word 'possible' ///////////
        Job possibleJob = Job.getInstance(getConf());
        possibleJob.setJobName("BigramPossible");
        possibleJob.setJarByClass(Driver.class);

        possibleJob.setMapperClass(PossibleMapper.class);
        possibleJob.setCombinerClass(PossibleCombiner.class);
        possibleJob.setReducerClass(PossibleReducer.class);	
        possibleJob.setNumReduceTasks(1);		//avoids overhead from creating multiple unused reducers and multiple empty output files

        possibleJob.setMapOutputKeyClass(Text.class);	//mapper and reducer output not the same 
        possibleJob.setMapOutputValueClass(Text.class);
        possibleJob.setOutputKeyClass(Bigram.class);	
        possibleJob.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(possibleJob, probOutPath);
        FileOutputFormat.setOutputPath(possibleJob, new Path(args[1]));

        return possibleJob.waitForCompletion(true) ? 0 : 1;
	}
}
