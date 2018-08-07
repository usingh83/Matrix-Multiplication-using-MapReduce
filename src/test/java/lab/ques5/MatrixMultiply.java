package lab.ques5;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiply {
	
    public static void main(String[] args) throws Exception {
    	if (args.length != 3) {
            System.err.println("Usage: MatrixMultiply <in_dir> <out_dir1> <out_dir2>");
            System.exit(2);
        }
    	Configuration conf = new Configuration();
        conf.set("m", "1000");
        conf.set("n", "2000");
        conf.set("p", "1000");
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean mapreduce = job.waitForCompletion(true);

		if (mapreduce) {
			Configuration conf1 = new Configuration();
			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf1, "TopTenFriends Phase 2");
			job1.setJarByClass(MatrixMultiply.class);
			job1.setMapperClass(Map2.class);
			job1.setReducerClass(Reduce2.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapOutputKeyClass(PairWritable.class);
			job1.setMapOutputValueClass(Text.class);
			
			job1.setOutputKeyClass(Text.class);
			
			job1.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			System.exit(job1.waitForCompletion(true) ? 0 : 1);

		}

    }
}