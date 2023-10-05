package bigram.probs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PossibleCombiner extends Reducer<Text, Text, Text, Text> {
	String maxSecond = null;
	Float maxProb = Float.MIN_VALUE;

    public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
    	StringBuilder outVal = new StringBuilder();

    	for(Text rawVal: values) {
    		String[] vals = rawVal.toString().split("\t");	//input = "possible": "<second>\t<probability>"
    		String second = vals[0];
    		Float curProb = Float.parseFloat(vals[1]);

    		if(curProb > maxProb) {		//find max probability key/value pair, save result for output
    			maxSecond = second;
    			maxProb = curProb;
    		}
    	}

    	outVal.append(maxSecond);
    	outVal.append("\t");
    	outVal.append(maxProb.toString());
    	context.write(new Text("possible"), new Text(outVal.toString()));
    }
}
