package hadoopjar;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class invertedIndex {

	
	
	
	//<itemId>, <numPurchased>, <pricePerUnit>, <shippingCost>
	
	
	public static void main(String[] args) {
		
		try {
			Scanner scan = new Scanner(new File(args[0]));
			
			
			while(scan.hasNextLine()) {
				String str = scan.nextLine();
				String[] arr = str.split(", ");			
				
				double purchased = Double.valueOf(arr[1]);
	
	            //emit(str, new StringBuilder(str).reverse().toString());
			}
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
}
