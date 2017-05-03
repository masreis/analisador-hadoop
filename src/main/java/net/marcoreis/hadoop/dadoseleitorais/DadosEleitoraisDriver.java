package net.marcoreis.hadoop.dadoseleitorais;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DadosEleitoraisDriver extends Configured
		implements Tool {
	private static Logger logger = Logger
			.getLogger(DadosEleitoraisDriver.class.getName());

	public Job criarJob(String inputDir, String outputDir)
			throws IOException {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(DadosEleitoraisDriver.class);
		job.setJobName("Dados eleitorais");
		//
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		//
		job.setMapperClass(DadosEleitoraisMapper.class);
		job.setReducerClass(DadosEleitoraisReducer.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//
		return job;
	}

	public static void main(String[] args) {
		try {
			int retorno = ToolRunner
					.run(new DadosEleitoraisDriver(), args);
			System.exit(retorno);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			logger.warn("Informe diretórios de entrada e saída");
		}
		Job job = criarJob(args[0], args[1]);
		try {
			FileSystem fs = FileSystem
					.get(job.getConfiguration());
			Path saida = new Path(args[1]);
			if (fs.exists(saida)) {
				fs.delete(saida, true);
			}
		} catch (Exception e) {
			System.err.println(
					"Erro crítico ao excluir o diretório de saída");
			System.exit(-1);
		}
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
