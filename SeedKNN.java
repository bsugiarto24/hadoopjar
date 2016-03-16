package hadoopjar;

//CSC 369: Distributed Computing
//Bryan Sugiarto

import org.apache.hadoop.io.IntWritable; 	// Hadoop's serialized int wrapper class
import org.apache.hadoop.io.LongWritable; 	// Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        	// Hadoop's serialized String wrapper class


import org.apache.hadoop.mapreduce.Mapper; 	// Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; 	// the MapReduce job class that is used a the driver


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;	// class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                				// Hadoop's implementation of directory path/filename

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;

//problem 5
public class SeedKNN {
	
	public static void main(String[] args) {	
		try {	
			Job  job = Job.getInstance();
			job.setJarByClass(SeedKNN.class);  
			FileInputFormat.addInputPath(job, new Path("/datasets/seeds_dataset.txt")); 
			FileOutputFormat.setOutputPath(job, new Path("./test/","output")); // put what you need as output file
			job.setMapperClass(SwitchMapper.class);
			job.setReducerClass(SwitchReducer.class);
			 
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
			job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

			job.setJobName("Seed KNN");
			job.waitForCompletion(true);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


//Mapper  Class Template
public static class SwitchMapper extends Mapper<LongWritable, Text, Text, Text > {

	
	
	//15.26	14.84	0.871	5.763	3.312	2.221	5.22	1
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	 
		String str =  value.toString();
		String text[] = str.split(" ");
		Double arr[] = new Double[text.length];
		
		for(int i = 0; i < text.length; i++) {
			double distance = 
			
		}
		
		
		//map date and energy
		context.write(new Text(key), new Text(text[0]));
		
	} // map
} // MyMapperClass


//Reducer Class Template
public static class SwitchReducer extends  Reducer< Text, Text, Text, Text> {

	@Override  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
			
		
		context.write(key, new Text(str));
	 } 
} // reducer


//Mapper  Class Template
public static class SwitchMapper2 extends Mapper<LongWritable, Text, Text, Text > {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	 
		String str =  value.toString();
		String text[] = str.split(" ");
		context.write(new Text(text[0]), new Text(text[1]));
		
	} // map
} // MyMapperClass


//Reducer Class Template
public static class SwitchReducer2 extends  Reducer< Text, Text, Text, Text> {

	@Override  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		//get max
		double max = 0;
		for (Text val : values) {
			String str = val.toString();
			double individual =  Double.parseDouble(str);
			if(max < individual)
				max = individual;
		}
		
		//map year and total energy
		context.write(key, new Text("" + max));
	 } 
} // reducer



} 







