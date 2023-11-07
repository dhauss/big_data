package res.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

import res.sampling.ResMapper;
import res.sampling.ResReducer;


public class TestReservoirSampling {
    MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;
    ReduceDriver<NullWritable, Text, IntWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, NullWritable, Text, IntWritable, Text> mapReduceDriver;
    
    @Before
    public void setUp() {
    	//resJob Mapper
    	ResMapper mapper = new ResMapper();
        mapDriver = new MapDriver<LongWritable, Text, NullWritable, Text>();
        mapDriver.setMapper(mapper);

        //resJob Reducer
        ResReducer reducer = new ResReducer();
        reduceDriver = new ReduceDriver<NullWritable, Text, IntWritable, Text>();
        reduceDriver.setReducer(reducer);
        
        //resJob MapReduce
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, NullWritable, Text, IntWritable, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer); 
    }
    
    
    @Test
    public void testMapper() throws IOException{
    	mapDriver.withInput(new LongWritable(1), new Text("some,entry\nother,,,,entry"));
    	mapDriver.withOutput(NullWritable.get(), new Text("some,entry"));
    	mapDriver.withOutput(NullWritable.get(), new Text("other,entry"));
    	mapDriver.runTest();
    }
    
    @Test
    public void testReducer() throws IOException{
    	List<Text> values = new ArrayList<>();
    	values.add(new Text("some,entry"));
    	values.add(new Text("other,entry"));
    	reduceDriver.withInput(NullWritable.get(), values);
    	reduceDriver.withOutput(new IntWritable(1), new Text("some,entry"));
    	reduceDriver.withOutput(new IntWritable(2), new Text("other,entry"));
    	reduceDriver.runTest();
    }
    
    @Test
    public void testMapperReducer() throws IOException{
    	mapReduceDriver.addInput(new LongWritable(1), new Text("some,entry\nother,,,,entry"));
    	mapReduceDriver.addOutput(new IntWritable(1), new Text("some,entry"));
    	mapReduceDriver.addOutput(new IntWritable(2), new Text("other,entry"));
    	mapReduceDriver.runTest();

    }
}
