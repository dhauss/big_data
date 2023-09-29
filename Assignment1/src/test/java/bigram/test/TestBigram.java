package bigram.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import bigram.probs.Bigram;
import bigram.probs.CountMapper;

public class TestBigram {
    MapDriver<LongWritable, Text, Bigram, LongWritable> mapDriver;
    
    @Before
    public void setUp() {
    	CountMapper mapper = new CountMapper();
        mapDriver = new MapDriver<LongWritable, Text, Bigram, LongWritable>();
        mapDriver.setMapper(mapper);
    }
    
    @Test
    public void testMapper() throws IOException{
    	mapDriver.withInput(new LongWritable(1), new Text("the, cat aNd;; the Blue. cat haT./;;"));
    	mapDriver.withOutput(new Bigram("the", "cat"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("cat", "and"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("and", "the"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("the", "blue"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("blue", "cat"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("cat", "hat"), new LongWritable(1));
    	mapDriver.runTest();
    }

}
