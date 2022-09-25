package org.mbds.hadoop.tp;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class Logs {
    static private PrintStream console_log;
    static private boolean node_was_initialized = false;
    private static void logPrint(String line){
        if(!node_was_initialized){
            try{
                console_log = new PrintStream(new FileOutputStream("/tmp/my_mapred_log.txt", true));
            } catch (FileNotFoundException e){
                return;
            }
            node_was_initialized = true;
        }
        console_log.println(line);
    }
}
