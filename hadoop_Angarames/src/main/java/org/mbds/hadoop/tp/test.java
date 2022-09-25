package org.mbds.hadoop.tp;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

public class test {
    public static void main(String[] args) throws Exception
    {
        Text  value = new Text("safouane ,");
        Text  value2 = new Text("oussama");

        ArrayList<Text> cars = new ArrayList<Text>();
        cars.add( new Text("Volvo"));
        cars.add( new Text("Voedlvo"));
        cars.add( new Text("Vorfglvo"));
        cars.add( new Text("Voghtlvo"));


        ArrayList<String> words = new ArrayList<String>();

        // Get the iterator
        Iterator<Text> it = cars.iterator();

        while(it.hasNext())   // Pour chaque valeur...
            words.add(it.next().toString()) ;



       // ArrayList<String> words = new ArrayList<String>();
        String wordsString = "" ;

        for(String s :words ){
            wordsString += s +" ,";
        }

        System.out.println( new Text(wordsString));






    }
}
