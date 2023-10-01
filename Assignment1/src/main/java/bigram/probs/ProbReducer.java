package bigram.probs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProbReducer extends Reducer<Bigram, LongWritable, Bigram, LongWritable> {

}
