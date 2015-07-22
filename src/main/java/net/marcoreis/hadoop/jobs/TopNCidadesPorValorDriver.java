package net.marcoreis.hadoop.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class TopNCidadesPorValorDriver extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(TopNCidadesPorValorDriver.class
			.getName());

	public Job criarJob(String inputDir, String outputDir) throws IOException {
		Job job = Job.getInstance();
		job.setJarByClass(TopNCidadesPorValorDriver.class);
		String nomeJob = "Job - Cidades com mais recursos";
		job.setJobName(nomeJob);
		//
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		//
		job.setMapperClass(MunicipiosBeneficiadosMapper.class);
		job.setReducerClass(MunicipiosBeneficiadosReducer.class);
		//
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		//
		getConf().addResource("configuracao-bolsa-familia.xml");
		Integer qtdCidades = Integer.parseInt(getConf().get(
				"quantidade.limite.cidades"));
		return job;
	}

	public static void main(String[] args) {
		try {
			int retorno = ToolRunner.run(new TopNCidadesPorValorDriver(), args);
			System.exit(retorno);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = criarJob(args[0], args[1]);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
