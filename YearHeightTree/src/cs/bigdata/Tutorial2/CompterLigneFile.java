package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "Input/arbres.csv";
		//Ouverture du fichier input
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			String line = br.readLine();
			line = br.readLine();
			int counter ;
			counter = 2 ;
			// Lire le fichier ligne par ligne
			while (line !=null){
				// Construire l'arbre avecc ses caracteristiques
				Tree.setparameters(line);
				// afficher l<arbre
				Tree.display(counter-1);
				// Lire la ligne suivante
				line = br.readLine();
				counter = counter + 1 ;
			}
		
		}
		finally{
			//Fermer le fichier
			in.close();
			fs.close();
		}
 
		
		
	}

}
