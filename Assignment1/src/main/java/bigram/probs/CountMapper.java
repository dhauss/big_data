package bigram.probs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, Bigram, LongWritable>{

	Text lastWord = null;
	Text curWord = new Text();
    private final static LongWritable one = new LongWritable(1);

    public void map(LongWritable key, Text value, Context context)
    		throws IOException, InterruptedException {

    	//normalize to lower case, remove all html tags, remove punctuation, and remove consecutive spaces
    	String line = value.toString().toLowerCase();
    	line = line.replaceAll("\\<.*?\\>", "");
    	line = line.replaceAll("[^\\sa-z0-9]", " ");
    	line = line.replaceAll("\\s{2,}", " ");

    	String[] words = line.split(" ");

    	if(words.length >= 2) {
    		for(String word: words) {
    			if(lastWord == null) {
    			lastWord = new Text(word);
    			}
    			else {
    				curWord.set(word);
    				context.write(new Bigram(lastWord, curWord), one);
    				context.getCounter(Driver.COUNTERS.BIGRAMCOUNT).increment(1L);	//increment global bigram counter
    				lastWord.set(curWord.toString());
    			}
    		}
    	}
    }
}
