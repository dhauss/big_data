package res.sampling;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
        Job resJob = Job.getInstance(getConf());
        resJob.setJobName("ReservoirSampling");
        resJob.setJarByClass(ResDriver.class);
        
        resJob.setMapperClass(ResMapper.class);
        resJob.setNumReduceTasks(0);

        resJob.setOutputKeyClass(LongWritable.class);
        resJob.setOutputValueClass(Text.class);

        return resJob.waitForCompletion(true) ? 0 : 1;
	}

}
