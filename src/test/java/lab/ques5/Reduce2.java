package lab.ques5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class Reduce2
  extends org.apache.hadoop.mapreduce.Reducer<PairWritable, Text, Text, Text> {
	@Override
	public void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		float value=0;
		int m=0,p=0;
		for (Text val : values) {
			value+= Float.parseFloat(val.toString());
		}
		if (value != 0.0f) {
			context.write(null,
					new Text("X"+","+key.p1+","+key.p2 + ","+ Float.toString(value)));
		}
	}
}