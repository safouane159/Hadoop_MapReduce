/*
  M2 MBDS - Hadoop - 2021

  --
  TP2: exemple de programme Hadoop - parcours de graphes avec Input/Output F.
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Classe Driver (contient le main du programme Hadoop).
public class Graph {


	// Notre classe MAP.
	// Les 4 types generques correspondent a:
	// 1 - Text: C'est le type de la cle d'entre.
	// 2 - GraphNodeWritable: C'est le type de la valeur d'entre.
	// 3 - Text: C'est le type de la cle de sortie.
	// 4 - GraphNodeWritable: C'est le type de la valeur de sortie.
	public static class GraphMap extends Mapper<Text,GraphNodeWritable,Text,GraphNodeWritable> {

		// La fonction MAP.
		// Note: Le type du premier argument correspond au premier type generique.
		// Note: Le type du second argument correspond au deuxieme type generique.
		// Note: L'objet Context nous permet d'ecrire les couples (cle,valeur).
		public void map(Text key, GraphNodeWritable value, Context context) throws IOException, InterruptedException {
			// key = la cle, dans ce cas, correspond a l'id du noeud (ex. "1")
			// value = le contenu de la ligne apres \t
			// SI NODE.COULEUR == "GRIS":
			if(value.color.equals("GRIS")){
			    // POUR CHAQUE FILS DANS NODE.CHILDREN:
				for(String fils_id : value.childs){ // ex. ["2", "5"]
					if (fils_id.equals(""))
						continue;
					// fils.profondeur = node.profondeur + 1
					int depth = value.depth + 1; // ex. 0 + 1
					// fils.couleur = gris
					// renvoyer (fils.id; fils)
					context.write(new Text(fils_id), new GraphNodeWritable("|GRIS|" + depth));
					// ex. ("2", "|GRIS|1")
					// ex. ("5", "|GRIS|1")
				}
			    // NODE.COULEUR="NOIR"
				value.color = "NOIR";
			}
			// Renvoyer (Node.id; node)
			context.write(key, value);
		}

	}

	// Notre classe REDUCE.
	// Les 4 types generques correspondent a:
	// 1 - Text: C'est le type de la cle d'entre.
	// 2 - Text: C'est le type de la valeur d'entre.
	// 3 - Text: C'est le type de la cle de sortie.
	// 4 - Text: C'est le type de la valeur de sortie.
	public static class GraphReduce extends Reducer<Text, GraphNodeWritable, Text, GraphNodeWritable>{

		// La fonction REDUCE.
		// Les arguments:
		//   La cle key,
		//   Un Iterable de toutes les valeurs qui sont associees a la cle en question
		//   Le contexte Hadoop (un handle qui nous permet de renvoyer le resultat a Hadoop).
		// Note: Le type du premier argument correspond au premier type generique.
		// Note: Le type du second argument Iterable correspond au deuxieme type generique.
		// Note: L'objet Context nous permet d'ecrire les couples (cle, valeur).
		public void reduce(Text key, Iterable<GraphNodeWritable> values, Context context) throws IOException, InterruptedException {	
			// ex. key = "1"
			// ex. values = ["2,5|NOIR|0", "|GRIS|1"]
			GraphNodeWritable node = new GraphNodeWritable();
			node.color = "BLANC";
			Iterator<GraphNodeWritable> i = values.iterator();
			while(i.hasNext()) {
				GraphNodeWritable other = i.next();
				// SI VALEUR.CHILDREN.LENGTH() > H_CHILDREN.LENGTH():
				if(other.childs.size() > node.childs.size()){
					// H_CHILDREN = VALEUR.CHILDREN
					node.childs = other.childs;
				}
				// SI VALEUR.COULEUR > H_COULEUR
				if(node.color.equals("BLANC") ||
				  (node.color.equals("GRIS") && other.color.equals("NOIR"))){
					// H_COULEUR = VALEUR.COULEUR
					node.color = other.color;
				}
				// SI VALEUR.PROFONDEUR > H_PROF
				if(other.depth > node.depth){
					node.depth = other.depth;
				}
			}
			context.write(key, node);
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
		job.setOutputValueClass(GraphNodeWritable.class);

		// Defini comment decouper/lire les donnees d'entree
		// KeyValueInputFormat decoupe le ou les fichiers d'entree par ligne
		// et s'attend a trouver des couples (clef;valeur) au sein de chacune des lignes
		// separes d'une tabulation
		// ex. Key    Value
		job.setInputFormatClass(GraphInputFormat.class);
		job.setOutputFormatClass(GraphOutputFormat.class);

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
