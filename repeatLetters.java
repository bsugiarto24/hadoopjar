import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class repeatLetters {
	
	
	public static void main(String[] args) {
		
		try {
			Scanner scan = new Scanner(new File(args[0]));
			
			
			while(scan.hasNextLine()) {
			String str = scan.nextLine();
	
	            for (int i = 0; i < str.length() - 1; i++) { 	
	            	//there is a double
	                if (str.charAt(i) == str.charAt(i+1)) {
	                    //emit(str, str.charAt(i));
	                    break;
	                } 
	            }
			}
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

}
