package br.mbs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;



public class BuildData {

	static   FileWriter fw = null;       
    static   BufferedWriter bw =null;
	static String[] states = {"RS","SC"};
	public static void main(String[] args) throws IOException {
		System.out.println("processing ... ");
		abreArquivo();
		for(int ind = 0; ind < Integer.MAX_VALUE; ind++) {
			StringBuilder data = new StringBuilder();
			String state = "";
			if(ind % 2 == 0) {
				state = states[0];
			}else {
				state = states[1];
			}
			data.append("NOME_").append(new Date()).append("\t").append(state).append("\n");
			escreveArquivo(data.toString());
		}
		closeArquivo();
		System.out.println("end processing");
	}
	
	private static void escreveArquivo(String data) throws IOException {
		bw.write(data);
		bw.flush();
	}
	
	private static void closeArquivo() throws IOException {
		 bw.close();			
	}
	
   
	private static void abreArquivo() throws IOException {


        // Cria arquivo
        File file = new File("states.txt");

        // Se o arquivo nao existir, ele gera
        if (!file.exists()) {
            file.createNewFile();
        }

         fw = new FileWriter(file.getAbsoluteFile(),true);
        
        bw = new BufferedWriter(fw);
        
	}
	
}
