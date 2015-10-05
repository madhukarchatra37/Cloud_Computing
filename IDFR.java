package com.TFIDF;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.TFIDF.*;

public class IDFR extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
    @Override 
    public void reduce( Text word,  Iterable<Text > counts,  Context context)
       throws IOException,  InterruptedException {
  	  int length=0;
  	  double tfidf,idf;
  	 String wordkey = word.toString();
  	 System.out.println(TFIDF.no_Of_Documents);
  	 
  	  String wordkeyl, valuekey,valuekey1="",temp_string="";
  	  ArrayList<String> list = new ArrayList<String>();
  	
       for ( Text count  : counts) {
    	   length  += 1;
    	   
    	   valuekey = count.toString();
    	   
    	   Pattern p =Pattern.compile("[a-z]{4}[0-9].[a-z]{3}");// File1.txt pattren
           Matcher m = p.matcher(valuekey);
           if(m.find()) {
        	   valuekey1=(valuekey.substring(0, m.end()));
        	   temp_string =valuekey.substring(m.end());
        	   
           }
                     
          
    	   wordkeyl=wordkey+"######"+valuekey1+","+temp_string;
    	   list.add(wordkeyl);
       }
       
       idf = Math.log10(Double.valueOf(TFIDF.no_Of_Documents)/Double.valueOf( length));
       System.out.println(wordkey+", "+length+", "+idf);
       for(String w: list) {
    	   tfidf = idf * Double.parseDouble(w.split(",")[1]);
    	   w= w.split(",")[0];
    	   word = new Text(w);
    	   context.write(word, new DoubleWritable(tfidf));
       } 
      
  	 
     
    }
 }
