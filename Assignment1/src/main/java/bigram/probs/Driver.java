package bigram.probs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
        Job job = Job.getInstance(getConf());
        job.setJobName("BigramCount");
        job.setJarByClass(Driver.class);

        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Bigram.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long counter = counters.findCounter(COUNTERS.BIGRAMCOUNT).getValue();
		System.out.printf("Bigram count: %d\n",
			      counters.findCounter(COUNTERS.BIGRAMCOUNT).getValue());

        return exitCode;
	}

}
