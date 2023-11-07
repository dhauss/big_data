package res.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import res.sampling.ResMapper;

public class TestReservoirSampling {
    MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;
    
    
    @Before
    public void setUp() {
    	//countJob Mapper
    	ResMapper mapper = new ResMapper();
        mapDriver = new MapDriver<LongWritable, Text, NullWritable, Text>();
        mapDriver.setMapper(mapper);
    }
    
    
    @Test
    public void testMapper() throws IOException{
    	mapDriver.withInput(new LongWritable(1), new Text("some,entry\nother,,,,entry"));
    	mapDriver.withOutput(NullWritable.get(), new Text("some,entry"));
    	mapDriver.withOutput(NullWritable.get(), new Text("other,entry"));
    	mapDriver.runTest();
    }

}
