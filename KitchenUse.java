
//CSC 369: Distributed Computing
//Bryan Sugiarto

import org.apache.hadoop.io.DoubleWritable;
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
import java.util.Date;
import java.util.HashSet;
import java.util.Scanner;

//problem 5
public class KitchenUse {
	
	public static void main(String[] args) {	
		try {		
			Job  job2 = Job.getInstance();
			job2.setJarByClass(KitchenUse.class);  
			FileInputFormat.addInputPath(job2, new Path("/datasets/household_power_consumption.txt")); // put what you need as input file
			FileOutputFormat.setOutputPath(job2, new Path("./test/","output")); // put what you need as output file
			job2.setMapperClass(SwitchMapper.class);
			job2.setReducerClass(SwitchReducer.class);
			 
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(DoubleWritable.class); // specify the output class (what reduce() emits) for key
			job2.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value			
			
			job2.setJobName("Kitchen Use");
			
			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			
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
		
		try {
			if(str.indexOf('0') != -1 && str.indexOf('?') != -1){
				double energy = Double.parseDouble(text[3]) *1000 / 60;
				double sub1 = Double.parseDouble(text[6]);
				double sub2 = Double.parseDouble(text[7]);
				double sub3 = Double.parseDouble(text[8]);
				
				String date[] = text[0].split("/");
				String time[] = text[1].split(":");
				energy = sub1;
				
				//map date and energy
				//16/10/2007;02:26:00;
				
				Date d = new Date();
				Integer.parseInt(time[1]);
				d.setHours(Integer.parseInt(time[0]));
				d.setMinutes(Integer.parseInt(time[1]));
				
				d.setMonth(Integer.parseInt(date[1]));
				d.setDate(Integer.parseInt(date[0]));
				d.setYear(Integer.parseInt(date[2]));
				
				
				long t = d.getTime();
			
				Date after =new Date(t + (5 * 60000));
				
				context.write(new Text(d.toString()), new Text("" + energy));
				context.write(new Text(after.toString()), new Text("" + energy));
			}
		}catch(Exception e){}
		}
		
	} // map
} // MyMapperClass


//Reducer Class Template
public static class SwitchReducer extends  Reducer< Text, Text, DoubleWritable, Text> {

	@Override  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		int count = 0;
		double val1 = 0, val2 = 0;
		
		for(Text txt : values) {
			String str  = txt.toString();
			
			if(count == 0)
				val1 = Double.parseDouble(str);
			else
				val2 = Double.parseDouble(str);
			count ++;
		}
		
		double diff = Math.abs(val1 - val2);
		
		context.write(new DoubleWritable(diff), key);
	 } 
} // reducer


}