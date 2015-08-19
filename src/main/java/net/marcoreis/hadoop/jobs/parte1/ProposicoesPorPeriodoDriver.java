package net.marcoreis.hadoop.jobs.parte1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProposicoesPorPeriodoDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Informe os parâmetros de entrada e saída");
			System.exit(-1);
		}
		//
		Job job = Job.getInstance();
		job.setJarByClass(ProposicoesPorPeriodoDriver.class);
		job.setJobName("Contador de proposições legislativas por período");
		//
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//
		job.setMapperClass(ProposicoesPorPeriodoMapper.class);
		job.setReducerClass(ProposicoesPorPeriodoReducer.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
