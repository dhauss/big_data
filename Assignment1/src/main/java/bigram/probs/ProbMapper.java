package bigram.probs;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbMapper extends Mapper<LongWritable, Text, Bigram, FloatWritable>{
	private long bigramCount;
	private Bigram outKey = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	  super.setup(context);
	  this.bigramCount  = context.getConfiguration().getLong(Driver.COUNTERS.BIGRAMCOUNT.name(), 0);
	}
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String line = value.toString();
    	String[] vals = line.split("\t");
    	outKey = new Bigram(vals[0], vals[1]);
    	float outVal = Float.parseFloat(vals[2]) / bigramCount;
		context.write(outKey, new FloatWritable(outVal));
    }
}
