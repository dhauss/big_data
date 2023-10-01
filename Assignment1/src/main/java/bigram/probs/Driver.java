package bigram.probs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counters;


public class Driver extends Configured implements Tool{
	public enum COUNTERS {		//global counter to keep track of total number of (non-unique) bigrams
		  BIGRAMCOUNT
		 }

	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		String countJobOut = "count-job-out";
		
		/////////// count number of each bigrams, global counter for total number of bigrams ///////////
        Configuration countConf = new Configuration();
        Job countJob = Job.getInstance(countConf);
        countJob.setJobName("BigramCount");
        countJob.setJarByClass(Driver.class);

        countJob.setMapperClass(CountMapper.class);
        countJob.setCombinerClass(CountReducer.class);
        countJob.setReducerClass(CountReducer.class);

        countJob.setOutputKeyClass(Bigram.class);
        countJob.setOutputValueClass(LongWritable.class);

        //if countOutPath already exists, remove existing file
        Path countOutPath = new Path(countJobOut);
		FileSystem fs = FileSystem.get(countConf);
		if (fs.exists(countOutPath)) {
			fs.delete(countOutPath, true);
		}

        FileInputFormat.addInputPath(countJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(countJob, countOutPath);

        countJob.waitForCompletion(true);

        /////////// mapper only job, calculate probability of each bigram based on total bigram count ///////////
        Counters counters = countJob.getCounters();
        Long counter = counters.findCounter(COUNTERS.BIGRAMCOUNT).getValue();	//save counter from countJob

        Configuration conf2 = new Configuration();
        Job probJob = Job.getInstance(conf2);
        probJob.setJobName("BigramProb");
        probJob.setJarByClass(Driver.class);
        probJob.getConfiguration().setLong(Driver.COUNTERS.BIGRAMCOUNT.name(), counter);	//set counter to probJob

        probJob.setMapperClass(ProbMapper.class);
        probJob.setNumReduceTasks(0);

        probJob.setOutputKeyClass(Bigram.class);
        probJob.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(probJob, new Path(countJobOut));
        FileOutputFormat.setOutputPath(probJob, new Path(args[1]));

        return probJob.waitForCompletion(true) ? 0 : 1;
	}
}
