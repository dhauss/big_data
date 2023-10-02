package wordCountTest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Sandbox.Map;
import Sandbox.Reduce;

public class TestDriver {
    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
    	Map mapper = new Map();
        mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);
        
        Reduce reducer = new Reduce();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);
        
        mapReduceDriver = new  MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException{
    	mapDriver.withInput(new LongWritable(1), new Text("the cat and the blue cat hat"));
    	mapDriver.withOutput(new Text("the"), new IntWritable(1));
    	mapDriver.withOutput(new Text("cat"), new IntWritable(1));
    	mapDriver.withOutput(new Text("and"), new IntWritable(1));
    	mapDriver.withOutput(new Text("the"), new IntWritable(1));
    	mapDriver.withOutput(new Text("blue"), new IntWritable(1));
    	mapDriver.withOutput(new Text("cat"), new IntWritable(1));
    	mapDriver.withOutput(new Text("hat"), new IntWritable(1));
    	mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
    	List<IntWritable> values = new ArrayList();
    	values.add(new IntWritable(1));
    	values.add(new IntWritable(1));
    	reduceDriver.withInput(new Text("cat"), values);
    	reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
    	reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException{
        mapReduceDriver.addInput(new LongWritable(1), new Text("the cat and the blue cat hat"));
        mapReduceDriver.addOutput(new Text("and"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("blue"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
        mapReduceDriver.addOutput(new Text("hat"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("the"), new IntWritable(2));
        mapReduceDriver.runTest();
    } 
}
