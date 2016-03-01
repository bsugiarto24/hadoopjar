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

public class activity extends Configured implements Tool {

  public static class JsonMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text    outputKey   = new Text();
    private Text 	outputValue	= new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     try { 
    	 JSONObject json = new JSONObject(value.toString());
    	 String user = json.getString("user");
    	 JSONObject action = json.getJSONObject("action");
		 String type = action.getString("actionType");
    	 
		 if(type.equals("gameStart")) {	 
			 context.write(new Text(user), new Text("start"));
		 }
		 
		 if(type.equals("gameEnd")) {	 
			 context.write(new Text(user), new Text(action.getString("gameStatus") ));
			 context.write(new Text(user), new Text("moves: " + action.getInt("actionNumber") ));
			 context.write(new Text(user), new Text("points: " + action.getInt("points") ));
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
			int highscore = 0, win = 0, start = 0, loss = 0, longestGame = 0;
			
			for (Text val : values) {
				context.write(key, val);
				if(val.toString().equals("won"))
					win++;
				if(val.toString().equals("start"))
					start++;
				if(val.toString().equals("Loss"))
					loss++;
				if(val.toString().contains("moves")){
					String input = val.toString();
					int moves = Integer.parseInt(input.substring(input.lastIndexOf(' ')).trim());
					
					if(moves > longestGame)
						longestGame = moves;
				}
				if(val.toString().contains("points")){
					String input = val.toString();
					int points = Integer.parseInt(input.substring(input.lastIndexOf(' ')).trim());
					
					if(points > highscore)
						highscore = points;
				}	
			}
		
			summary.put("games", start);
			summary.put("won", win);
			summary.put("lost", loss);
			summary.put("highscore", highscore);
			summary.put("longestGame", longestGame);
			
			/*
			 * {
				"games": <gamesPlayed>,
				"won": <gamesWon>,
				"lost": <gamesLost>,
				"highscore": <highestGameEndScore>,
				"longestGame": <largestNMoves>
			   }
			 */
			
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

    job.setJarByClass(activity.class);
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
    int res = ToolRunner.run(conf, new activity(), args);
    System.exit(res);
  }
}
