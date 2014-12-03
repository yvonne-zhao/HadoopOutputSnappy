//Author: Yvonne Zhao
//Email: yvonnezhj@hotmail.com
//Date: 3 Dec 2014

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OutputCompressionSnappy  extends Configured implements Tool {
	public static class CompressionMap extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
				context.write(NullWritable.get(), new Text(value));
		}
	}

	public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  int res = ToolRunner.run(conf, new OutputCompressionSnappy(), args);
		  System.exit(res);

	}
	
	@Override
	public int run(String[] args) throws Exception {       
		String inputpath =args[0].trim();
		String outputpath=args[1].trim();
		
		// When implementing tool        
		Configuration conf = this.getConf();         
		// Create job        
		Job job = new Job(conf, "Snappy Compression");        
		job.setJarByClass(OutputCompressionSnappy.class);     
		
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(new Path(outputpath))) { // if file exist,delete it
			fs.delete(new Path(outputpath), true);
		}
		
		// Setup MapReduce job        
		// Do not specify the number of Reducer        
		job.setMapperClass(CompressionMap.class);
		//Skip Reducer
		//job.setReducerClass(Reducer.class);        
		
		// Specify key / value        
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        // Execute job and return status        
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
