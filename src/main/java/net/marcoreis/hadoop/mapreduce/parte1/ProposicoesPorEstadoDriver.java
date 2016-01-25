package net.marcoreis.hadoop.mapreduce.parte1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProposicoesPorEstadoDriver {

    public static void main(String[] args) throws Exception {
	String arquivoEntrada = System.getProperty("user.home") + "/dados/legislativo/entrada/proposicoes.csv";
	String diretorioSaida = System.getProperty("user.home") + "/dados/legislativo/saida";
	//
	Job job = Job.getInstance();
	job.setJarByClass(ProposicoesPorEstadoDriver.class);
	job.setJobName("Contador de proposições por estado");
	//
	FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
	FileOutputFormat.setOutputPath(job, new Path(diretorioSaida));
	//
	job.setMapperClass(ProposicoesPorEstadoMapper.class);
	job.setReducerClass(ProposicoesPorEstadoReducer.class);
	//
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	//
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	//
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
