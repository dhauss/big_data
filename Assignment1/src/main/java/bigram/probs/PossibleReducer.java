package bigram.probs;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PossibleReducer extends Reducer<Text, Text, Bigram, FloatWritable> {

}
