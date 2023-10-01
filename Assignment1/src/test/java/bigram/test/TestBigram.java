package bigram.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import bigram.probs.Bigram;
import bigram.probs.CountMapper;
import bigram.probs.CountReducer;
import bigram.probs.Driver;



public class TestBigram {
    MapDriver<LongWritable, Text, Bigram, LongWritable> mapDriver;
    ReduceDriver<Bigram, LongWritable, Bigram, LongWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Bigram, LongWritable, Bigram, LongWritable> mapReduceDriver;


    
    @Before
    public void setUp() {
    	CountMapper mapper1 = new CountMapper();
        mapDriver = new MapDriver<LongWritable, Text, Bigram, LongWritable>();
        mapDriver.setMapper(mapper1);

        CountReducer reducer1 = new CountReducer();
        reduceDriver = new ReduceDriver<Bigram, LongWritable, Bigram, LongWritable>();
        reduceDriver.setReducer(reducer1);

        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Bigram, LongWritable, Bigram, LongWritable>();
        mapReduceDriver.setMapper(mapper1);
        mapReduceDriver.setReducer(reducer1);  
    }

    @Test
    public void testMapper() throws IOException{
    	mapDriver.withInput(new LongWritable(1), new Text("the, cat aNd;;    the Blue. tHe cat haT./;;"));
    	mapDriver.withOutput(new Bigram("the", "cat"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("cat", "and"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("and", "the"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("the", "blue"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("blue", "the"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("the", "cat"), new LongWritable(1));
    	mapDriver.withOutput(new Bigram("cat", "hat"), new LongWritable(1));
    	mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
    	List<LongWritable> values = new ArrayList<>();
    	values.add(new LongWritable(1));
    	values.add(new LongWritable(1));
    	reduceDriver.withInput(new Bigram("the", "cat"), values);
    	reduceDriver.withOutput(new Bigram("the", "cat"), new LongWritable(2));
    	reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException{
    	mapReduceDriver.addInput(new LongWritable(1), new Text("the, cat aNd;;    the Blue. tHe cat haT./;;"));
    	mapReduceDriver.addOutput(new Bigram("and", "the"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("blue", "the"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("cat", "and"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("cat", "hat"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("the", "blue"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("the", "cat"), new LongWritable(2));
    	mapReduceDriver.runTest();
    } 
}
