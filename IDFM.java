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

public class IDFM extends Mapper<LongWritable, Text, Text, Text>  {
	
	    private final static IntWritable one = new IntWritable(1);
	    
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	   
	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	    		
	      String line = lineText.toString();
	      System.out.println(line);
	     
	      Text currentW1 = new Text();
	      Text currentW2 = new Text();
	      String key,value;
	     
	        
	        	
	        	
	        	String[] arr=line.split("######");
	        	key= arr[0];
	        	value=arr[1];
	        	
	 	        currentW1 = new Text(key);
	 	        currentW2 = new Text(value);
	 	        context.write(currentW1, currentW2);

	       
	   
	    }
	  }
