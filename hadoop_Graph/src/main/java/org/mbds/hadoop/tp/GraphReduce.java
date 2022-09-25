package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountReduce.java: classe REDUCE.
*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;
import java.io.IOException;


// Notre classe REDUCE - templatee avec un type generique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class GraphReduce extends Reducer<Text, Text, Text, Text> {
    // La fonction REDUCE elle-meme. Les arguments: la clef key (d'un type generique K), un Iterable de toutes les valeurs
    // qui sont associees a la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le resultat a Hadoop).
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Pour parcourir toutes les valeurs associees a la clef fournie.
        Iterator<Text> i = values.iterator();
        String Vert = "";
        String nextVert = "";
        String color = "";
        int position = 0;
        boolean FirstElement = true;

        while (i.hasNext()) {
            Vert = i.next().toString();
            String[] Columns = Vert.split("\\|");

            if (FirstElement == true) {
                nextVert = Columns[0];
                position = Integer.valueOf(Columns[2]);
                FirstElement = false;
                color = Columns[1];
            } else {
                if (position < Integer.valueOf(Columns[2])) {
                    position = Integer.valueOf(Columns[2]);
                    color = "GRIS";
                }
            }


        }

        context.write(key, new Text(nextVert+"|" + color+ "|"+position));


        // On renvoie le couple (clef;valeur) constituee de notre clef key et du total, au format Text.

    }
}