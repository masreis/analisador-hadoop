package net.marcoreis.hadoop.mapreduce.parte1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaiorValorDeProposicoesDriver {
    public static void main(String[] args) throws Exception {
	String arquivoEntrada = System.getProperty("user.home") + "/dados/legislativo/entrada/proposicoes.csv";
	String diretorioSaida = System.getProperty("user.home") + "/dados/legislativo/saida";
	//
	Job job = Job.getInstance();
	//
	job.setJarByClass(MaiorValorDeProposicoesDriver.class);
	job.setJobName("Maior valor de proposições legislativas por parlamentar");
	//
	FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
	FileOutputFormat.setOutputPath(job, new Path(diretorioSaida));
	//
	job.setMapperClass(MaiorValorDeProposicoesMapper.class);
	job.setReducerClass(MaiorValorDeProposicoesReducer.class);
	//
	job.setNumReduceTasks(1);
	//
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	//
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
