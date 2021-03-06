
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



//problem 3
public class scores {
	
	
	public static void main(String[] args) {
		
		try {
			// step 1: get a new MapReduce Job object
			Job  job = Job.getInstance();  //  job = new Job() is now deprecated
			 
			// step 2: register the MapReduce class
			job.setJarByClass(scores.class);  
			
			//  step 3:  Set Input and Output files
			FileInputFormat.addInputPath(job, new Path(args[0])); // put what you need as input file
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
public static class SwitchMapper extends Mapper<LongWritable, Text, LongWritable, Text > {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	 
		//String text[] =  value.toString().split(",");
				 
		//3902	305, 114, 37.78, 5.38
		
		context.write(key, value);
		
		
	} // map
} // MyMapperClass


//Reducer Class Template

public static class SwitchReducer extends  Reducer< LongWritable, Text, LongWritable, Text> {


	@Override 
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		//for (Text val : values) 
		//	context.write(key, new Text(val));
		//<itemId>, <numPurchased>, <pricePerUnit>, <shippingCost>
		long numPurchased;
		double profit, shipping, revenue;
		
		numPurchased = 0;
		profit = shipping = revenue = 0;
		
		try{
			for (Text val : values) {
				String str = val.toString();
				String text[] =  str.split(",");
				
				numPurchased += Long.parseLong(text[1].trim());			
				revenue += Long.parseLong(text[1].trim()) * Double.parseDouble(text[2].trim());
				shipping += Double.parseDouble(text[3].trim());
			}
			
			if(numPurchased < 100) {
				profit = ((shipping + revenue) * 1.025);
				profit *= 100;
				profit = (long) profit;
				context.write(key, new Text(numPurchased + ", " + profit/100));
			}else {
				profit += (long) ((shipping + revenue) * 1.0225 * 100 / numPurchased);
				profit += (long) ((shipping + revenue) * 1.03 * (numPurchased - 100) / numPurchased);
				profit *= 100;
				profit = (long) profit;
				context.write(key, new Text(numPurchased + ", " + profit/100));
			}
		}catch(Exception e) {
			
		}
				
	}
 
} // reducer


} // MyMapReduceDriver

