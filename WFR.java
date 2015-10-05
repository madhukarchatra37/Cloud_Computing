package com.TFIDF;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WFR extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
    @Override 
    public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
       throws IOException,  InterruptedException {

    	
  	 double total = 0;
  	 
     for ( DoubleWritable count  : counts) {
        total  += count.get();
     }
     total = 1+ Math.log10(total);
     context.write(word,new DoubleWritable( total));
    }
 }
