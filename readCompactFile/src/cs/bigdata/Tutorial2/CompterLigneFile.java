package cs.bigdata.Tutorial2;



import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "Input/isd-history.txt";
		//Ouvrir Le fichier input
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			String line = br.readLine();
			int counter ;
			counter = 1;
			// lire ligne par ligne
			while (line !=null){
				
				// sauter les 22 premieres lignes
				if (counter > 22) {
				// contruire la station avec les parametres
				Station.setparameters(line);
				// afficher la station
				Station.display(counter-1);
				}
				// lire la ligne suivante
				line = br.readLine();
				counter = counter + 1 ;
			}
		
		}
		finally{
			//Fermer le fichier input
			in.close();
			fs.close();
		}
 
		
		
	}

}
