
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
public class PowerComp {
	
	public static void main(String[] args) {	
		try {
			
			Job  job = Job.getInstance();
			job.setJarByClass(PowerComp.class);  
			FileInputFormat.addInputPath(job, new Path("/datasets/household_power_consumption.txt")); 
			//FileInputFormat.addInputPath(job, new Path("./power.txt")); 
			FileOutputFormat.setOutputPath(job, new Path("./test/","temp")); // put what you need as output file
			job.setMapperClass(SwitchMapper.class);
			job.setReducerClass(SwitchReducer.class);
			 
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
			job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

			job.setJobName("Power Comp");
			job.waitForCompletion(true);
			
			
			Job  job2 = Job.getInstance();
			job2.setJarByClass(PowerComp.class);  
			FileInputFormat.addInputPath(job2, new Path("./test/temp/part-r-00000")); // put what you need as input file
			FileOutputFormat.setOutputPath(job2, new Path("./test/","output")); // put what you need as output file
			job2.setMapperClass(SwitchMapper2.class);
			job2.setReducerClass(SwitchReducer2.class);
			 
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
			job2.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

			job2.setJobName("Power Comp");
			
			System.exit(job2.waitForCompletion(true) ? 0:1);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


//Mapper  Class Template
public static class SwitchMapper extends Mapper<LongWritable, Text, Text, Text > {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	 
		String str =  value.toString();
		String text[] = str.split(";");
		
		if(str.indexOf('0') != -1 && str.indexOf('?') == -1){
			double energy = Double.parseDouble(text[3]) *1000 / 60;
			double sub1 = Double.parseDouble(text[6]);
			double sub2 = Double.parseDouble(text[7]);
			double sub3 = Double.parseDouble(text[8]);
			String flag = "";
			
			flag += (sub2 > 1)? "Y" : "N";
			flag += (sub3 > 1)? "Y" : "N";
			
			
			//map date and energy
			context.write(new Text(text[0]), new Text(flag + ":" + energy));
		}
		
	} // map
} // MyMapperClass


//Reducer Class Template
public static class SwitchReducer extends  Reducer< Text, Text, Text, Text> {

	@Override  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		//get total
		double total = 0;
		boolean flag[] = new boolean[3];
		
		for (Text val : values) {
			String str[] = val.toString().split(":");
			
			int count = 0;
			for(int i = 0; i < str[0].length(); i++) {
				if(str[0].charAt(i) == 'Y')
					flag[i] = true;
			}
			total += Double.parseDouble(str[1]);
		}
		
		
		int count = 0;
		for(boolean bool : flag) {
			count += (bool)? 1 : 0;
		}
		
		
		//get year
		String str =  key.toString();
		String text[] = str.split("/");
		

		//map year and total energy
		context.write(new Text("" + count), new Text("" + total));
	 } 
} // reducer


//Mapper  Class Template
public static class SwitchMapper2 extends Mapper<LongWritable, Text, Text, Text > {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		String str =  value.toString();
		String text[] = str.split("\t");
		context.write(new Text(text[0]), new Text(text[1]));
		
	} // map
} // MyMapperClass


//Reducer Class Template
public static class SwitchReducer2 extends  Reducer< Text, Text, Text, Text> {

	@Override  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		//get max
		double total = 0;
		for (Text val : values) {
			String str = val.toString();
			double individual =  Double.parseDouble(str);
			total += individual;
		}
		
		//map # of devices and total energy
		context.write(key, new Text("" + total));
	 } 
} // reducer



} 







