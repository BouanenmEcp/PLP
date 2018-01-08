package cs.bigdata.Tutorial2;

public class Station {
	
	private static String name  ;
	private static String FIPS ;
	private static String altiture ;

	
	static  void setparameters (String line){
		
		if (line != null ) {	
			
			name = line.substring(13, 13+29);
			FIPS = line.substring(43, 43+2);
			altiture = line.substring(74, 74+7);
		
		}else {
			
			name = "#NA" ;
			FIPS = "#NA" ;
			altiture = "#NA" ;
		}
	}

	static void display( int n){
		System.out.println("Station " + n + " --- name : " + name + " --- FIPS : "+ FIPS + " --- altiture : "+ altiture);
	}
}
