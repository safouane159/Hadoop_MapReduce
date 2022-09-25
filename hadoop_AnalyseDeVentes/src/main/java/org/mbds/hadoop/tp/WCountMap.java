package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountMap.java: classe MAP.
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// Notre classe MAP.
public class WCountMap extends Mapper<Object, Text, Text, FloatWritable> {
    private static final IntWritable ONE = new IntWritable(1);

    // La fonction MAP elle-m��me.
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        // implemenation anagramme

        Configuration conf = context.getConfiguration();
        String TypeOperation = conf.get("TypeOperation");


        String valueString = value.toString();
        String[] Columns = valueString.split(",");

        Text myKeyTEXT = new Text();
        FloatWritable myValueLOng = new FloatWritable(0);

        if (Columns[0].equals("Region")) {
             myKeyTEXT = new Text("Region");



        } else{

            if (TypeOperation.equals("RegDuM")) {
                myKeyTEXT = new Text(Columns[0]);

            } else if (TypeOperation.equals("Pays")) {
                myKeyTEXT = new Text(Columns[1]);

            } else if (TypeOperation.equals("tyIt")) {
                myKeyTEXT = new Text(Columns[2]);


            } else if (TypeOperation.equals("QtdeVt")) {
                myKeyTEXT = new Text(Columns[0]+"-"+Columns[3]);


            }
             myValueLOng = new FloatWritable(Float.parseFloat(Columns[13]));


        }




        context.write(myKeyTEXT, myValueLOng);


    }
}
