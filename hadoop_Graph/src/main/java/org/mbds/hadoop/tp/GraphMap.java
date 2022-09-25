package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountMap.java: classe MAP.
*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.StringTokenizer;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.mbds.hadoop.tp.Logs;


// Notre classe MAP.
public class GraphMap extends Mapper<Text, Text, Text, Text> {
    private static final IntWritable ONE = new IntWritable(1);
    static private PrintStream console_log;
    static private boolean node_was_initialized = false;

    // La fonction MAP elle-m��me.
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Un StringTokenizer va nous permettre de parcourir chacun des mots de la ligne qui est pass��e
        // �� notre op��ration MAP.
        String keyString = key.toString();
        String valueString = value.toString();
        String[] Columns = valueString.split("\\|");


        logPrint("===================== value of first  :  " + Columns[0]);
        String[] Vertices = Columns[0].split(",");
        logPrint("===================== Vertices.length :  " + Vertices.length);
        if (Columns[1].equals("GRIS")) {

            context.write(new Text(key.toString()), new Text(Columns[0] + "|Noir|" + Columns[2]));

            for (int i = 0; i < Vertices.length; i++) {

                int sum = Integer.valueOf(Columns[2]) + 1;
                logPrint("============================================= Writting f1 ==============================");
                logPrint("===================== v1 : " + Vertices[0]);
                logPrint("===================== v2 : " + Vertices[1]);
                logPrint("===================== value : " + "|GRIS|" + sum);
                context.write(new Text(Vertices[i]), new Text("|GRIS|" + sum));

            }

        } else {
            logPrint("============================================= Writting 1 ==============================");
            logPrint("===================== key : " + key.toString());
            logPrint("===================== value : " + Columns[0] + "|" + Columns[1] + "|" + Columns[2]);
            context.write(new Text(key.toString()), new Text(Columns[0] + "|" + Columns[1] + "|" + Columns[2]));

        }


    }

    private static void logPrint(String line) {
        if (!node_was_initialized) {
            try {
                console_log = new PrintStream(new FileOutputStream("/tmp/my_mapred_log.txt", true));
            } catch (FileNotFoundException e) {
                return;
            }
            node_was_initialized = true;
        }
        console_log.println(line);
    }
}
