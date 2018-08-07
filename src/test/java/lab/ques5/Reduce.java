package lab.ques5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class Reduce
  extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String[] value;
		int m=0,n=0,p=0;
		float a_ij = 0;
		float b_jk = 0;
		for (Text val : values) {
			value = val.toString().split(",");
			if (value[0].equals("A")) {
				m=Integer.parseInt(value[1]);
				n=Integer.parseInt(value[2]);
				a_ij=Float.parseFloat(value[3]);
			} else {
				p=Integer.parseInt(value[2]);
				n=Integer.parseInt(value[1]);
				b_jk=Float.parseFloat(value[3]);
			}
		}
		float result = a_ij * b_jk;
		if (result != 0.0f) {
			context.write(null,
					new Text(m + "," + p + ","+ Float.toString(result)));
		}
	}
}