package res.sampling;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ResDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ResDriver(), args);
        System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
        if(args.length !=2){
            System.err.println("Usage: <input path> <output path>");
            return -1;
        }
		
        Job resJob = Job.getInstance(getConf());
        resJob.setJobName("ReservoirSampling");
        resJob.setJarByClass(ResDriver.class);
        
        resJob.setMapperClass(ResMapper.class);
        resJob.setReducerClass(ResReducer.class);
        resJob.setNumReduceTasks(1);

        resJob.setMapOutputKeyClass(LongWritable.class);
        resJob.setMapOutputValueClass(Text.class);
        resJob.setOutputKeyClass(IntWritable.class);
        resJob.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(resJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(resJob, new Path(args[1]));

        return resJob.waitForCompletion(true) ? 0 : 1;
	}

}
