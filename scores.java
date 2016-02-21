
	    //		

//CSC 369: Distributed Computing
//Bryan Sugiarto

//Java Hadoop Template

//Section 1: Imports


//Data containers for Map() and Reduce() functions
//You would import the data types needed for your keys and values
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
import java.util.Scanner;



//problem 1
public class scores {
	
	
	public static void main(String[] args) {
		
		try {
			//Scanner scan = new Scanner(new File(args[0]));
			
			// step 1: get a new MapReduce Job object
			Job  job = Job.getInstance();  //  job = new Job() is now deprecated
			 
			// step 2: register the MapReduce class
			job.setJarByClass(scores.class);  
			
			//  step 3:  Set Input and Output files
			FileInputFormat.addInputPath(job, new Path("prog3.txt")); // put what you need as input file
			FileOutputFormat.setOutputPath(job, new Path("./test/","output")); // put what you need as output file
			
			// step 4:  Register mapper and reducer
			job.setMapperClass(SwitchMapper.class);
			job.setReducerClass(SwitchReducer.class);
			  
			//  step 5: Set up output information
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(LongWritable.class); // specify the output class (what reduce() emits) for key
			job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
			
			// step 6: Set up other job parameters at will
			job.setJobName("Program 3");
			
			// step 7:  ?
			
			// step 8: profit
			System.exit(job.waitForCompletion(true) ? 0:1);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


//Mapper  Class Template
	// Need to replace the four type labels there with actual Java class names
public static class SwitchMapper extends Mapper<LongWritable, Text, LongWritable, Text > {

//@Override   // we are overriding Mapper's map() method
//map methods takes three input parameters
//first parameter: input key 
//second parameter: input value
//third parameter: container for emitting output key-value pairs

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	 
		String str =  value.toString().toLowerCase();
				 
		context.write(key, new Text(str));
		
		
	} // map
} // MyMapperClass


//Reducer Class Template

public static class SwitchReducer extends  Reducer< LongWritable, Text, LongWritable, Text> {


	@Override 
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
	
		String str = "";
		
		for (Text val : values) {
			str = val.toString();
		}
		
		context.write(key, new Text(str));

	 } 
} // reducer


} // MyMapReduceDriver

