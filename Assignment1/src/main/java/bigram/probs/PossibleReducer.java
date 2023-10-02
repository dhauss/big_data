package bigram.probs;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PossibleReducer extends Reducer<Text, Text, Bigram, FloatWritable> {
	Bigram maxBigram = new Bigram();
	Float maxFloat = Float.MIN_VALUE;

    public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

    	for(Text rawVal: values) {
    		String rawString = rawVal.toString();
    		String[] vals = rawString.split("\t");
    		Float curFloat = Float.parseFloat(vals[1]);
    		if(curFloat > maxFloat) {
    			maxBigram = new Bigram("possible", vals[0]);
    			maxFloat = curFloat;
    		}
    	}

    	context.write(maxBigram, new FloatWritable(maxFloat));

    }

}
