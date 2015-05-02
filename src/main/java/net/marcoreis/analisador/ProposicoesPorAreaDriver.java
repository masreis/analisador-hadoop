package net.marcoreis.analisador;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProposicoesPorAreaDriver {

	private static Logger logger = Logger
			.getLogger(ProposicoesPorAreaDriver.class.getName());

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Informe os parâmetros de entrada e saída");
			System.exit(-1);
		}
		//
		Job job = Job.getInstance();
		job.setJarByClass(ProposicoesPorAreaDriver.class);
		job.setJobName("Contador de proposições legislativas");
		//
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//
		job.setMapperClass(ProposicoesPorAreaMapper.class);
		job.setReducerClass(ProposicoesPorAreaReducer.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//
		try {
			FileSystem fs = FileSystem.get(job.getConfiguration());
			Path saida = new Path(args[1]);
			if (fs.exists(saida)) {
				logger.log(Level.WARNING, "Diretorio de saida apagado");
				fs.delete(saida, true);
			}
		} catch (Exception e) {
			System.err.println("Erro crítico ao excluir o diretório de saída");
			System.exit(-1);
		}
		//
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
