package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountMap.java: classe MAP.
*/

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// Notre classe MAP.
public class WCountMap extends Mapper<Object, Text, Text, Text> {
    private static final IntWritable ONE = new IntWritable(1);

    // La fonction MAP elle-m��me.
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        // implemenation anagramme


        String valueString = value.toString();

        String[] Letters = valueString.split("");

        Arrays.sort(Letters);

        Text myKeyTEXT = new Text(String.join("", Letters));


        context.write(myKeyTEXT, value);



    }
}
