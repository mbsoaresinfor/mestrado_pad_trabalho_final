package br.mbs.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


//https://www.programcreek.com/java-api-examples/?code=junneyang%2Fxxhadoop%2Fxxhadoop-master%2Fmr%2Fsrc%2Fmain%2Fjava%2Fcom%2Fxcompany%2Fxproject%2Fmr%2Fflowpartition%2FFlowPartitionMapper.java#
public class CountPersonByState {

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private int totalToken = 2;
    private String separator = "\t";
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	
    	
    	String[] tokens = value.toString().split(separator);
    	
    	boolean tokensOk = tokens != null && tokens.length == totalToken;
    	if(tokensOk) {
    		String name = tokens[0];
    		String state = tokens[1];
    		word.set(state);
    		context.write(word, one);
    	}else {
    		System.err.println("Tokens invalids");
    	}
    	

    }
}

    public static void log(String message) {
    	System.out.println("DEBUG: " + message);
    }
    
    
    private static String stateToProcessor = "RS";  
    
public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {

	private IntWritable result = new IntWritable();
	

    public void reduce(Text key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {

    	int sum = 0;
    	if(key.toString().equalsIgnoreCase(stateToProcessor)) {

    		for (IntWritable val : values) {
		        sum += val.get();
	        }
    		result.set(sum);        
            context.write(key, result);
    	}else {
    		log("key ignored: " + key);
    	}
        

    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
        System.err.println("Usage: CountPersonByState <in> <out> <state>");
        System.exit(2);
    }
    log("Starting...");
    Job job = new Job(conf, "CountPersonByState");
    job.setJarByClass(CountPersonByState.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    if(otherArgs[2]  != null) {
    	stateToProcessor = otherArgs[2].toUpperCase();
    }
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}