package res.sampling;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class ResReducer extends Reducer<LongWritable, Text, NullWritable, Text>{

}
