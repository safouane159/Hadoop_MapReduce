package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountMap.java: classe driver (contient le main du programme).
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;


// Note classe Driver (contient le main du programme Hadoop).
public class Graph
{
	// Le main du programme.
	public static void main(String[] args) throws Exception
	{
		// Cr���� un object de configuration Hadoop.
		Configuration conf=new Configuration();
		// Permet �� Hadoop de lire ses arguments g��n��riques, r��cup��re les arguments restants dans ourArgs.
		String[] ourArgs=new GenericOptionsParser(conf, args).getRemainingArgs();
		// Obtient un nouvel objet Job: une t��che Hadoop. On fourni la configuration Hadoop ainsi qu'une description
		// textuelle de la t��che.
		Job job=Job.getInstance(conf, "Compteur de mots v1.0");

		// D��fini les classes driver, map et reduce.
		job.setJarByClass(Graph.class);
		job.setMapperClass(GraphMap.class);
		job.setReducerClass(GraphReduce.class);

		// D��fini types clefs/valeurs de notre programme Hadoop.
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// D��fini les fichiers d'entr��e du programme et le r��pertoire des r��sultats.
		// On se sert du premier et du deuxi��me argument restants pour permettre �� l'utilisateur de les sp��cifier
		// lors de l'ex��cution.
		FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));

		// On lance la t��che Hadoop. Si elle s'est effectu��e correctement, on renvoie 0. Sinon, on renvoie -1.
		if(job.waitForCompletion(true))
			System.exit(0);
		System.exit(-1);
	}
}