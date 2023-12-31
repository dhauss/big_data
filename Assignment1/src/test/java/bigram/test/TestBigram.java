package bigram.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import bigram.probs.Bigram;
import bigram.probs.CountMapper;
import bigram.probs.PossibleMapper;
import bigram.probs.CountReducer;
import bigram.probs.PossibleReducer;

public class TestBigram {
    MapDriver<LongWritable, Text, Bigram, LongWritable> mapDriver;
    MapDriver<LongWritable, Text, Text, Text> possibleMapDriver;
    ReduceDriver<Bigram, LongWritable, Bigram, LongWritable> reduceDriver;
    ReduceDriver<Text, Text, Bigram, FloatWritable> possibleReduceDriver;
    MapReduceDriver<LongWritable, Text, Bigram, LongWritable, Bigram, LongWritable> mapReduceDriver;
    
    @Before
    public void setUp() {
    	//countJob Mapper
    	CountMapper mapper1 = new CountMapper();
        mapDriver = new MapDriver<LongWritable, Text, Bigram, LongWritable>();
        mapDriver.setMapper(mapper1);

        //possibleJob Mapper
        PossibleMapper mapper2 = new PossibleMapper();
        possibleMapDriver = new MapDriver<LongWritable, Text, Text, Text>();
        possibleMapDriver.setMapper(mapper2);

        //countJob Reducer
        CountReducer reducer1 = new CountReducer();
        reduceDriver = new ReduceDriver<Bigram, LongWritable, Bigram, LongWritable>();
        reduceDriver.setReducer(reducer1);

        //possibleJob Reducer
        PossibleReducer reducer2 = new PossibleReducer();
        possibleReduceDriver = new ReduceDriver<Text, Text, Bigram, FloatWritable>();
        possibleReduceDriver.setReducer(reducer2);

        //countJob MapReduce
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Bigram, LongWritable, Bigram, LongWritable>();
        mapReduceDriver.setMapper(mapper1);
        mapReduceDriver.setReducer(reducer1);  
    }

    @Test
    public void testMapper() throws IOException{
    	mapDriver.withInput(new LongWritable(1), new Text("the, cat aNd;;    the <div> Blue.</div> tHe cat haT./;;"));
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
    public void TestMapper2() throws IOException{
    	possibleMapDriver.withInput(new LongWritable(1), new Text("possible\tbigram\t0.00234"));
    	possibleMapDriver.withInput(new LongWritable(2), new Text("impossible\tbigram\t0.00234"));
    	possibleMapDriver.withOutput(new Text("possible"), new Text("bigram\t0.00234"));
    	possibleMapDriver.runTest();
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
    public void testReducer2() throws IOException{
    	List<Text> values = new ArrayList<>();
    	values.add(new Text("bigram\t0.00234"));
    	values.add(new Text("biggie\t0.000234"));
    	values.add(new Text("bigrams\t0.00235"));
    	possibleReduceDriver.withInput(new Text("possible"), values);
    	possibleReduceDriver.withOutput(new Bigram("possible", "bigrams"), new FloatWritable(0.00235F));
    	possibleReduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException{
    	mapReduceDriver.addInput(new LongWritable(1), new Text("the, cat aNd;;  <p>  the Blue. tHe cat haT./;;"));
    	mapReduceDriver.addOutput(new Bigram("and", "the"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("blue", "the"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("cat", "and"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("cat", "hat"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("the", "blue"), new LongWritable(1));
    	mapReduceDriver.addOutput(new Bigram("the", "cat"), new LongWritable(2));
    	mapReduceDriver.runTest();
    } 
}
