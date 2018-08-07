package lab.ques5;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map2
  extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, PairWritable, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] X = line.split(",");
		PairWritable outputKey = new PairWritable(Long.parseLong(X[0]),Long.parseLong(X[1]));
		Text outputValue = new Text();
		outputValue.set(X[2]);
		context.write(outputKey, outputValue);
	}
}