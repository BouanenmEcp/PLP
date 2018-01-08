package cs.bigdata.Tutorial2;

public class Tree {
	
	private static String year ;
	private static String height ;

	static String cvsSplitBy = ";";
	
	static  void setparameters (String line){
		
		if (line != null ) {		
			String[] fields = line.split(cvsSplitBy);
			if (fields[5].isEmpty()) {
				year = "#NA" ;
			}else {
				year = fields[5] ;
			}
			if (fields[6].isEmpty()){
				height = "#NA" ;
			}else {
				height = fields[6];
			}
		}else {
			year = "#NA" ;
			height = "#NA" ;
		}
	}

	static void display( int n){
		System.out.println("Tree " + n + " --- year : " + year + " --- Height : "+ height);
	}
}
