package com.TFIDF;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public  class WFM extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
    private final static DoubleWritable one  = new DoubleWritable( 1);
    

    private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

    public void map( LongWritable offset,  Text lineText,  Context context)
      throws  IOException,  InterruptedException {
  	  FileSplit filesplit = (FileSplit)context.getInputSplit();
	    	String filename = filesplit.getPath().getName();
       String line  = lineText.toString();
       System.out.println(line);
       Text currentW  = new Text();

       for ( String word  : WORD_BOUNDARY .split(line)) {
          if (word.isEmpty()) {
             continue;
          }
          word+="######"+filename;
          currentW  = new Text(word);
          context.write(currentW,one);
       }
    }
 }
