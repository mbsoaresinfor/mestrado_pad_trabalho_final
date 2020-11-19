package br.mbs.without.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;



public class CountPersonByState {

	
	 private static int totalToken = 2;
	 private static String separator = "\t";
	 private static String stateToProcessor = "RS";
	 public static void main(String[] args) {

		
	    if (args.length != 3) {
	        System.err.println("Usage: CountPersonByState <in> <out> <state>");
	        System.exit(2);
	    }
	    
	    System.out.println("args[0] = " + args[0]);
	    System.out.println("args[1] = " + args[1]);
	    if(args[2] != null) {
	    	System.out.println("args[2] = " + args[2]);	 
	    	stateToProcessor = args[2].toUpperCase();
	    }
		System.out.println("Processing...");
	    int cont = 0;
	    BufferedReader br = null;
        FileReader fr = null;
		try {		      
			
	        fr = new FileReader(args[0]);
            br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {            	
		        String[] tokens = line.split(separator);		    	
		    	boolean tokensOk = tokens != null && tokens.length == totalToken;
		    	if(tokensOk) {
		    		String name = tokens[0];
		    		String state = tokens[1];
		    		if(stateToProcessor.equals(state)) {
		    			cont++;
		    		}
		    	}else {		    
		    		System.err.println("Tokens invalids");
		    	}
            } 
		     writeFile(stateToProcessor + " = " + cont,args[1]);
		    } catch (Exception e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }finally {
		    	 try {
	                if (br != null)
	                    br.close();

	                if (fr != null)
	                    fr.close();
	            } catch (IOException ex) {
	                System.err.format("IOException: %s%n", ex);
	            }
			}
	}
	
	private static void writeFile(String data,String namefile) throws IOException {
       
        File file = new File(namefile);

        // Se o arquivo nao existir, ele gera
        if (!file.exists()) {
            file.createNewFile();
        }

        // Prepara para escrever no arquivo
        FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
        BufferedWriter bw = new BufferedWriter(fw);
 
        bw.write(data);
        bw.close();
        
	}


}
