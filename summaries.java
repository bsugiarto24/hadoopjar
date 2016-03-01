// CSC 369 Winter 2016
// Multi Line JSON handling example
// Chris Wu
 
// run with  hadoop jar job.jar MultilineJsonJob -libjars /path/to/json-20151123.jar,/path/to/json-mapreduce-1.0.jar /input /output


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;

public class summaries extends Configured implements Tool {

  public static class JsonMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text    outputKey   = new Text();
    private Text 	outputValue	= new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     try { 
    	 JSONObject json = new JSONObject(value.toString());
    	 int game = json.getInt("game");
    	 context.write(new Text(game + ""), new Text("user: " + json.getString("user") ));
    	 
    	 JSONObject action = json.getJSONObject("action");
    	
    	 if(action != null){
    		 String type = action.getString("actionType");
        	 
    		 if(type.equals("Move"))
    			 context.write(new Text(game + ""), new Text("regular"));
    		 if(type.equals("SpecialMove"))
    			 context.write(new Text(game + ""), new Text("special"));
    		 
    		 context.write(new Text(game + ""), new Text("points: " + action.getInt("pointsAdded") ));
    		 context.write(new Text(game + ""), new Text("move: " + action.getInt("actionNumber") ));
    		 context.write(new Text(game + ""), new Text("user: " + json.getString("user") ));
    		 
    		 if(type.equals("gameEnd")) {	 
    			 context.write(new Text(game + ""), new Text("status: " + action.getString("gameStatus") ));
    		 }
    	 }
    	 
    } catch (Exception e) {System.out.println(e); }
    }
  }

  public static class JsonReducer
      extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
    	try {
			JSONObject summary = new JSONObject(); 												
			int regular = 0, special = 0, points = 0;
			String user = "", outcome = "In Progress";
			
			for (Text val : values) {
				if(val.toString().equals("regular"))
					regular++;
				if(val.toString().equals("special"))
					special++;
				if(val.toString().equals("special"))
					special++;
				if(val.toString().contains("user")){
					String input = val.toString();
					user = input.substring(input.lastIndexOf('u'));
				}
				if(val.toString().contains("points")){
					String input = val.toString();
					points += Integer.parseInt(input.substring(input.lastIndexOf(' ')).trim());
				}	
				
				if(val.toString().contains("status")){
					context.write(new Text("key"), new Text("value"));
					String input = val.toString();
					outcome = input.substring(input.lastIndexOf(' ')).trim();
				}	
			}
		
			summary.put("user", user);
			summary.put("moves", special + regular);
			summary.put("regular", regular);
			summary.put("special", special);
			summary.put("outcome", outcome);
			summary.put("score", points);
			summary.put("perMove", (double) points / (special + regular));
			
			context.write(key, new Text(summary.toString(2)));	
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
	}
}

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = super.getConf();
    Job job = Job.getInstance(conf, "multiline json job");

    job.setJarByClass(summaries.class);
    job.setMapperClass(JsonMapper.class);
    job.setReducerClass(JsonReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(MultiLineJsonInputFormat.class);
    MultiLineJsonInputFormat.setInputJsonMember(job, "game");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new summaries(), args);
    System.exit(res);
  }
}
