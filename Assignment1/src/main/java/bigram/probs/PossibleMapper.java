package bigram.probs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PossibleMapper extends Mapper<LongWritable, Text, Text, Text> {
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] vals = line.split("\t");
		String first = vals[0];
		String second = vals[1];
		Float prob = Float.parseFloat(vals[2]);
		
		if(first.equals("possible")) {
			String outVal = second + '\t' + prob.toString();
			context.write(new Text(first), new Text(outVal));
		}
    }
}
