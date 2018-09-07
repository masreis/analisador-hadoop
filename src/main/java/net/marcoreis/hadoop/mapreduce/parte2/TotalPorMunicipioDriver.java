package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * @author marco
 *
 *         Este job mostra as cidades que mais receberam recursos
 * 
 *
 */
public class TotalPorMunicipioDriver extends Configured
		implements Tool {
	private static Logger logger = Logger
			.getLogger(TotalPorMunicipioDriver.class.getName());

	public Job criarJob(String inputDir, String outputDir)
			throws IOException {
		// getConf().addResource("configuracao-job.xml");
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TotalPorMunicipioDriver.class);
		String nomeJob = job.getConfiguration()
				.get("nome.job.municipios.beneficiados");
		if (nomeJob == null) {
			nomeJob = "Total por município";
		}
		job.setJobName(nomeJob);
		//
		FileInputFormat.addInputPath(job, new Path(inputDir));
		String time = DateFormatUtils.format(
				System.currentTimeMillis(),
				"yyyy-MM-dd-hh-mm-ss");
		FileOutputFormat.setOutputPath(job,
				new Path(outputDir + "-" + time));
		//
		job.setMapperClass(TotalPorMunicipioMapper.class);
		job.setReducerClass(TotalPorMunicipioReducer.class);
		// job.setCombinerClass(MunicipiosBeneficiadosReducer.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//
		return job;
	}

	public static void main(String[] args) {
		try {
			int retorno = ToolRunner
					.run(new TotalPorMunicipioDriver(), args);
			System.exit(retorno);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// String entrada =
		// "/home/marco/dados/bolsa-familia/entrada/201505_BolsaFamiliaFolhaPagamento.csv";
		// String saida = "/home/marco/dados/bolsa-familia/saida";
		if (args.length < 2) {
			logger.warn(
					"Informe diretórios de entrada e saída");
		}
		Job job = criarJob(args[0], args[1]);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
