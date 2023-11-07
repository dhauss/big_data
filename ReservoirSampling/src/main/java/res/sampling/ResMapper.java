package res.sampling;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;


public class ResMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	//size of random sample
	static final int K = 10000;
	//array to save current random sample in memory
	String[] reservoir = new String[K];
	//Random object for computing j, which determines if an element will replace a reservoir entry
	Random rand = new Random();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	//split csv file by line breaks to create array of rows
    	String[] rows = value.toString().split("\n");

    	int i = 0;
    	//iterate through input, apply reservoir sampling algo to fill reservoir
    	for(String row: rows) {
    		//initially fill reservoir with first K elements
    		if(i < K) {
    			reservoir[i] = row;
    			i++;
    		} else {
    		//compute random int, j, to decide if ith element will replace jth element in reservoir
    			int j = rand.nextInt(i);
    			if(j < K) {
    				reservoir[j] = row;
    			}
    			i++;
    		}
    	}

    	// write final reservoir to output
    	for(String out: reservoir) {
    		// if reservoir is not filled, avoid writing null entries to context
    		if(out == null) {
    			break;
    		} else {
    			//NullWritable key to send all data to single reducer (output size limited to number of mappers * K)
    			//replaces multiple commas with single comma for output readability given plethora of null entries in dataset
    			//context.write(new IntWritable(1), new Text(out.replaceAll(",{2,}", ",")));
    			context.write(NullWritable.get(), new Text(out.replaceAll(",{2,}", ",")));
    		}
    	}
    }
}
