/*
Bigram class adapted from https://www.udemy.com/course/learn-by-example-hadoop-mapreduce/
*/
package bigram.probs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Bigram implements WritableComparable<Bigram>{
	private Text first;
	private Text second;

	public Bigram() {
		this.first = new Text();
		this.second = new Text();
	}

	public Bigram(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public Bigram(String first, String second) {
		this(new Text(first), new Text(second));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.first.write(out);
		this.second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.first.readFields(in);
		this.second.readFields(in);
	}

	@Override
	public int compareTo(Bigram other) {
		int compFirst = first.compareTo(other.first);
		if (compFirst != 0) {
			return compFirst;
		}
		else {
			return second.compareTo(other.second);
		}
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 113 + second.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if(other instanceof Bigram) {
			Bigram otherBG = (Bigram) other;
			return first.equals(otherBG.first) && second.equals(otherBG.second);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return this.first.toString() + "\t" + this.second.toString();
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public void setSecond(Text second) {
		this.second = second;
	}
}
