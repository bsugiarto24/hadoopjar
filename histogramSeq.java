
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;

import org.json.*;
import org.json.JSONTokener;

public class histogramSeq {

	
	public static void main(String[] args){
		String file = "out.txt";
		HashMap<String, Integer> locs = new HashMap<String, Integer>();
		
		long start = System.currentTimeMillis();

		JSONTokener t;
		JSONParser parser;
		
		try {
			t = new JSONTokener(new FileReader(new File(file)));
			JSONArray arr = new JSONArray(t);
			
			for(int i = 0; i< arr.length(); i++) {
				JSONObject move = arr.getJSONObject(i).getJSONObject("action");
				
				if(move.has("location")) {
					JSONObject loc = move.getJSONObject("location");
					String location = "(" + loc.getInt("x") + "," + loc.getInt("y") + ")";
					
					if(locs.containsKey(location)) {
						locs.put(location, locs.get(location) + 1);
					}else {
						locs.put(location, 1);
					}
					
				}
				
			}
			long end = System.currentTimeMillis();
			
			System.out.println("time: " + (end-start));
			System.out.println("map: " + locs.toString());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
