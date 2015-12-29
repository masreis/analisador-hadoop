package net.marcoreis.hadoop.jobs.parte1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProposicoesPorAreaDriver {

	public static void main(String[] args) throws Exception {
		String arquivoEntrada = System.getProperty("user.home") + "/dados/legislativo/entrada/proposicoes.csv";
		String diretorioSaida = System.getProperty("user.home") + "/dados/legislativo/saida";
		//
		Job job = Job.getInstance();
		job.setJarByClass(ProposicoesPorAreaDriver.class);
		job.setJobName("Contador de proposições legislativas");
		//
		FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
		FileOutputFormat.setOutputPath(job, new Path(diretorioSaida));
		//
		job.setMapperClass(ProposicoesPorAreaMapper.class);
		job.setReducerClass(ProposicoesPorAreaReducer.class);
		//
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
