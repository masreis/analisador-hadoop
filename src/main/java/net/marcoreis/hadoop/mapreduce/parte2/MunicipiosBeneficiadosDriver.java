package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class MunicipiosBeneficiadosDriver extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(MunicipiosBeneficiadosDriver.class.getName());

	public Job criarJob(String inputDir, String outputDir) throws IOException {
		getConf().addResource("configuracao-job.xml");
		Job job = Job.getInstance(getConf());
		job.setJarByClass(MunicipiosBeneficiadosDriver.class);
		String nomeJob = job.getConfiguration().get("nome.job.municipios.beneficiados");
		job.setJobName(nomeJob);
		//
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		//
		job.setMapperClass(MunicipiosBeneficiadosMapper.class);
		job.setReducerClass(MunicipiosBeneficiadosReducer.class);
		// job.setCombinerClass(MunicipiosBeneficiadosReducer.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(3);
		//
		return job;
	}

	public static void main(String[] args) {
		try {
			int retorno = ToolRunner.run(new MunicipiosBeneficiadosDriver(), args);
			System.exit(retorno);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String entrada = "/home/marco/dados/bolsa-familia/entrada/201505_BolsaFamiliaFolhaPagamento.csv";
		String saida = "/home/marco/dados/bolsa-familia/saida";
		Job job = criarJob(entrada, saida);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
