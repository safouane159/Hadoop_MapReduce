/*
  M2 MBDS - Hadoop - 2021

  --
  TP2: exemple de programme Hadoop - parcours de graphes.
  --
*/
package mbds;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Classe Driver (contient le main du programme Hadoop).
public class Graph {


	// Notre classe MAP.
	// Les 4 types generques correspondent a:
	// 1 - Text: C'est le type de la cle d'entre.
	// 2 - Text: C'est le type de la valeur d'entre.
	// 3 - Text: C'est le type de la cle de sortie.
	// 4 - Text: C'est le type de la valeur de sortie.
	public static class GraphMap extends Mapper<Text,Text,Text,Text> {

		// La fonction MAP.
		// Note: Le type du premier argument correspond au premier type generique.
		// Note: Le type du second argument correspond au deuxieme type generique.
		// Note: L'objet Context nous permet d'ecrire les couples (cle,valeur).
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// key = la cle, dans ce cas, correspond a l'id du noeud (ex. "1")
			// value = le contenu de la ligne apres \t
			String node = value.toString(); // ex. "2,5|GRIS|0"
			String[] splitted_node = node.split("\\|"); // ex. ["2,5", "GRIS", "0"]

			// SI NODE.COULEUR == "GRIS":
			if(splitted_node[1].equals("GRIS")){
			    // POUR CHAQUE FILS DANS NODE.CHILDREN:
				for(String fils_id : splitted_node[0].split(",")){ // ex. ["2", "5"]
					if (fils_id.equals(""))
						continue;
					// fils.profondeur = node.profondeur + 1
					int depth = Integer.parseInt(splitted_node[2]) + 1; // ex. 0 + 1
					// fils.couleur = gris
					// renvoyer (fils.id; fils)
					context.write(new Text(fils_id), new Text("|GRIS|" + Integer.toString(depth)));
					// ex. ("2", "|GRIS|1")
					// ex. ("5", "|GRIS|1")
				}
			    // NODE.COULEUR="NOIR"
				splitted_node[1] = "NOIR";
			}
			String new_value = splitted_node[0] + "|" + splitted_node[1] + "|" + splitted_node[2];
			// Renvoyer (Node.id; node)
			context.write(key, new Text(new_value));
		}

	}

	// Notre classe REDUCE.
	// Les 4 types generques correspondent a:
	// 1 - Text: C'est le type de la cle d'entre.
	// 2 - Text: C'est le type de la valeur d'entre.
	// 3 - Text: C'est le type de la cle de sortie.
	// 4 - Text: C'est le type de la valeur de sortie.
	public static class GraphReduce extends Reducer<Text, Text, Text, Text>{

		// La fonction REDUCE.
		// Les arguments:
		//   La cle key,
		//   Un Iterable de toutes les valeurs qui sont associees a la cle en question
		//   Le contexte Hadoop (un handle qui nous permet de renvoyer le resultat a Hadoop).
		// Note: Le type du premier argument correspond au premier type generique.
		// Note: Le type du second argument Iterable correspond au deuxieme type generique.
		// Note: L'objet Context nous permet d'ecrire les couples (cle, valeur).
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			// ex. key = "1"
			// ex. values = ["2,5|NOIR|0", "|GRIS|1"]
			String children = "";
			String color = "BLANC";
			int depth = -1;
			Iterator<Text> i = values.iterator();
			while(i.hasNext()) {
				String node = i.next().toString(); // ex. "2,5|NOIR|0"
				String[] splitted_node = node.split("\\|"); // ex. ["2,5", "NOIR", "0"]
				// SI VALEUR.CHILDREN.LENGTH() > H_CHILDREN.LENGTH():
				if(splitted_node[0].length() > children.length()){
					// H_CHILDREN = VALEUR.CHILDREN
					children = splitted_node[0];
				}
				// SI VALEUR.COULEUR > H_COULEUR
				if(color.equals("BLANC") ||
				  (color.equals("GRIS") && splitted_node[1].equals("NOIR"))){
					// H_COULEUR = VALEUR.COULEUR
					color = splitted_node[1];
				}
				int new_depth = Integer.parseInt(splitted_node[2]);
				// SI VALEUR.PROFONDEUR > H_PROF
				if(new_depth > depth){
					depth = new_depth;
				}
			}
			// ex. ("1", "2,5|NOIR|1")
			context.write(key, new Text(children + "|" + color + "|" + Integer.toString(depth)));
		}

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// Cree un object de configuration Hadoop.
		Configuration conf = new Configuration();
		// Permet a Hadoop de lire ses arguments generiques, recupere les arguments restants dans ourArgs.
		String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// Obtient un nouvel objet Job: une tache Hadoop. On fourni la configuration Hadoop ainsi qu'une description
		// textuelle de la tache.
		Job job = Job.getInstance(conf, "Graph Job v1");
		
		// Defini les classes driver, map et reduce.
		job.setJarByClass(Graph.class);
		job.setMapperClass(GraphMap.class);
		job.setReducerClass(GraphReduce.class);
		
		// Defini types cle/valeurs de notre programme Hadoop.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Defini comment decouper/lire les donnees d'entree
		// KeyValueInputFormat decoupe le ou les fichiers d'entree par ligne
		// et s'attend a trouver des couples (clef;valeur) au sein de chacune des lignes
		// separes d'une tabulation
		// ex. Key    Value
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// Defini les fichiers d'entree du programme et le repertoire des resultats.
		// On se sert du premier et du deuxieme argument restants pour permettre a l'utilisateur de les specifier
		// lors de l'execution.
		FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));
		
		// On lance la tache Hadoop. Si elle s'est effectuee correctement, on renvoie 0. Sinon, on renvoie -1.
		if(job.waitForCompletion(true)) {
			System.exit(0);
		}
		System.exit(1);
	}
}
