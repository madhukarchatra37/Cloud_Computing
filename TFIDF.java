package com.TFIDF;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDF extends Configured implements Tool{

	public static  int no_Of_Documents =0;
	public static void main(String[] args)throws Exception {
		int res;
		
			res = ToolRunner.run( new TFIDF(), args);
		
	      System .exit(res);
	}
	public int run( String[] args) throws  Exception {
		FileSystem fs = FileSystem.get(getConf());
		   Path pt = new Path("Input");
		   ContentSummary cs = fs.getContentSummary(pt);
		   no_Of_Documents = (int) cs.getFileCount();
		   
		//noOfDocuments = args.length -2;
		
	      Job job  = Job .getInstance(getConf(), " WF ");
	      job.setJarByClass( this .getClass());

	      FileInputFormat.addInputPaths(job,  args[0]);
	     
	      FileOutputFormat.setOutputPath(job,new Path(args[1]) );
	      job.setMapperClass( WFM .class);
	      job.setReducerClass( WFR .class);
	      job.setOutputKeyClass( Text .class);
	      job.setOutputValueClass( DoubleWritable .class);
	      job.waitForCompletion(true);
	      Job job2  = Job .getInstance(getConf(), " TIIDF ");
	      job2.setJarByClass( this .getClass());

	      FileInputFormat.addInputPath(job2,new Path(args[1]));
	      FileOutputFormat.setOutputPath(job2,  new Path(args[2]));
	      
	      job2.setMapperClass( IDFM.class);
	      job2.setReducerClass( IDFR.class);
	      
	      job2.setMapOutputKeyClass(Text.class);
	      job2.setMapOutputValueClass(Text.class);
	      job2.setOutputKeyClass( Text .class);
	      job2.setOutputValueClass( DoubleWritable .class); 

	      return job2.waitForCompletion( true)  ? 0 : 1;
	   }

}
