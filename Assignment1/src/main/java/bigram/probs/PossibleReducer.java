package bigram.probs;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PossibleReducer extends Reducer<Text, Text, Bigram, FloatWritable> {
	Bigram maxBigram = new Bigram();
	Float maxProb = Float.MIN_VALUE;

    public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

    	for(Text rawVal: values) {
    		String[] vals = rawVal.toString().split("\t");	//input = "possible": "<second>\t<probability>"
    		String second = vals[0];
    		Float curProb = Float.parseFloat(vals[1]);

    		if(curProb > maxProb) {		//find max probability key/value pair, save result for output
    			maxBigram = new Bigram(key.toString(), second);
    			maxProb = curProb;
    		}
    	}

    	context.write(maxBigram, new FloatWritable(maxProb));
    }

}
