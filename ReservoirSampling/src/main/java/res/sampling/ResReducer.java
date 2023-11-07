package res.sampling;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class ResReducer extends Reducer<NullWritable, Text, IntWritable, Text>{
	//size of random sample
	static final int K = 10000;
	//array to save current random sample in memory
	Text[] reservoir = new Text[K];
	//Random object for computing j, which determines if an element will replace a reservoir entry
	Random rand = new Random();

    public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

    	int i = 0;
    	for(Text row: values) {
    		//initially fill reservoir with first K elements
    		if(i < K) {
    			// create local Text object to avoid Hadoop object pointer bugs
    			reservoir[i] = new Text(row.toString());
    			i++;
    		} else {
    			//compute random int, j, to decide if ith element will replace jth element in reservoir
    			int j = rand.nextInt(i);
    			if(j < K) {
        			// create local Text object to avoid Hadoop object pointer bugs
    				reservoir[j] = new Text(row.toString());
    			}
    			i++;
    		}
    	}

    	i = 1;
    	// write final reservoir to output
    	for(Text out: reservoir) {
    		// if reservoir is not filled, avoid writing null entries to context
    		if(out == null) {
    			break;
    		} else {
    			//use IntWritable as key to enumerate rows, unlikely K will ever exceed 2 billion
    			context.write(new IntWritable(i), out);
    			i++;
    		}
    	}
    }
}
