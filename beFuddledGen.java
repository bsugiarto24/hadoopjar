import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class beFuddledGen {
	
	public static int random(int size) {
		return (int) (size * Math.random());
	}

	private static int randomGameSize() {
		int r = random(10);
		if(r < 1) 			return 70 + random(31);
		else				return 9 + random(61);
	}
	
	public static void main(String[] args) {
		int NUMBEROFGAMES = 20;
		int obj = 500000;
		String file = "out.txt";
		
		System.out.println("obj: " + obj);

		try {
			
			JSONArray log = new JSONArray();
			
			FileWriter fw = new FileWriter(file);
			
			fw.write("[\n");
			
			Game[] games = new Game[NUMBEROFGAMES];
			
			//number of moves the game will have
			int[] moves = new int[NUMBEROFGAMES];
			int[] done = new int[NUMBEROFGAMES];
			int gameStartedId = 0;
			
		
			//for each game, make a new game
			for(int i = 0; i< NUMBEROFGAMES; i++) {
				games[i] = new Game();
				moves[i] = randomGameSize();
				done[i] = -1;
			}
			
			//loop through json objects
			while(obj-- > 0) {
				int index = random(NUMBEROFGAMES);
				
				//selects a finished game
				if(moves[index] == -1) {
					//if games are done
					//System.out.println(Arrays.toString(moves));
					if(Arrays.equals(moves, done)) {
						//restart all 100 games.
						gameStartedId -= NUMBEROFGAMES;
						for(int i = 0; i< NUMBEROFGAMES; i++) {
							games[i] = new Game();
							moves[i] = randomGameSize();
						}
					}	
				}
				//selects a game about to end
				else if(moves[index] == 0 ){
					//log.put(games[index].endGame());
					fw.write(games[index].endGame().toString(2));
					fw.write(",\n");
					moves[index]--;
				}
				//selects a game that is about to start
				else if(games[index].move == 1){
					//log.put(games[gameStartedId].generateNewGame());
					fw.write(games[gameStartedId].generateNewGame().toString(2));
					fw.write(",\n");
					gameStartedId++;
				}
				//selects a game that does a regular move
				else{
					//log.put(games[index].move());
					fw.write(games[index].move().toString(2));
					fw.write(",\n");
					moves[index]--;	
				}
				
			}

			//fw.write(log.toString(2));
			
			fw.write("]\n");
			fw.flush();
			
			
			fw.close();
			

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
}



class Game {
	
	public static int gameNumber = 1;
	public static HashSet<String> users = new HashSet<String>();
	
	private String user;
	public JSONArray log;
	public int move;
	private int points, gameId, specialCount;
	private String[] specials = {"Shuffle", "Invert", "Clear", "Rotate"};
	private boolean[] specialBools = {false, false, false, false};
	
	
	public Game() throws JSONException {
		log = new JSONArray();
		specialCount = 0;
		user = "u" + random(10000);
		
		while(users.contains(user)){
			user = "u" + random(10000);
		}
		
		users.add(user);
		points = 0;
		move = 1;
		gameId = Game.gameNumber;
		Game.gameNumber++;
		
		
		/* Adds Moves to the Game */
		/*int gameSize = randomGameSize();
		generateNewGame();
		while(gameSize-- != 0) {
			int type = random(100);
			
			if(type < 6 && specialCount != 4)
				specialMove();
			else
				regularMove();
		}
		endGame();*/
		
	}
	
	//randomizes special or regular move
	public JSONObject move() throws JSONException {
		int type = random(100);
		
		if(type < 6 && specialCount != 4)
			return specialMove();
		else
			return regularMove();	
	}
	
		
	//creates a move Object
	private JSONObject generateMove() throws JSONException {
		JSONObject move = new JSONObject();
		move.put("user", user);
		move.put("game", gameId);
		return move;
	}
	
	//creates the action object
	private JSONObject generateActionObject(String type) throws JSONException {
		JSONObject action = new JSONObject();
		action.put("actionType", type);
		action.put("actionNumber", move);
		move++;
		return action;
	}
	
	//creates the location object
	private JSONObject generateLocation() throws JSONException{
		JSONObject location = new JSONObject();
		location.put("x", randomLocation());
		location.put("y", randomLocation());	
		return location;
	}
	
	//start a new game
	public JSONObject generateNewGame() throws JSONException {
		JSONObject newGame = generateMove();
		newGame.put("action", generateActionObject("gameStart"));
		//log.put(newGame);
		
		return newGame;
	}
	
	//end game
	public JSONObject endGame() throws JSONException {
		JSONObject endGame = generateMove();
		JSONObject action = generateActionObject("gameEnd");
		
		action.put("gameStatus", (random(2) == 1)? "Win": "Loss");
		action.put("points", points);
		
		endGame.put("action", action);
		
		users.remove(user);
		//log.put(endGame);
		return endGame;
			
	}
	
	//regular move
	public JSONObject regularMove() throws JSONException {
		JSONObject regular = generateMove();
		
		int movePoints = random(41) - 20;
		points += movePoints;
		
		JSONObject location = generateLocation();
		JSONObject action = generateActionObject("Move");
		
		action.put("location", location);
		action.put("pointsAdded", movePoints);
		action.put("points", points);
		
		regular.put("action", action);
		//log.put(regular);
		return regular;
	}
	
	//special move
	public JSONObject specialMove() throws JSONException {
		JSONObject special = generateMove();
		specialCount++;
		int moveType = random(5);
		
		if(moveType < 2)
			moveType = 0;
		else
			moveType--;
		
		String type = "";
		
		//type is already used?
		while(specialBools[moveType] == true) {
			moveType = random(5);
			if(moveType < 2)
				moveType = 0;
			else
				moveType--;
		}
		
		type = specials[moveType];
		specialBools[moveType] = true;
		int movePoint = random(41) - 20;
		points += movePoint;
		
		JSONObject action = generateActionObject("SpecialMove");
		action.put("pointsAdded", movePoint);
		action.put("points", points);
		action.put("move", type);
		special.put("action", action);
		
		return special;
		//log.put(special);
	}
	
	private static int random(int size) {
		return (int) (size * Math.random());
	}
	
	private static int randomLocation() {
		int r = random(10);
		if(r < 1) 			return random(3) + 1;
		else if(r > 18) 	return random(3) + 18;
		else				return 4 + random(15);
	}
	
	private static int randomGameSize() {
		int r = random(10);
		if(r < 1) 			return 70 + random(31);
		else				return 9 + random(61);
	}
	
}
