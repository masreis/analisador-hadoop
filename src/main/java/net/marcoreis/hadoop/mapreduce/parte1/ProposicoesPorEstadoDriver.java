package net.marcoreis.hadoop.mapreduce.parte1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProposicoesPorEstadoDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Informe os parâmetros de entrada e saída");
			System.exit(-1);
		}
		//
		Job job = Job.getInstance();
		job.setJarByClass(ProposicoesPorEstadoDriver.class);
		job.setJobName("Média de proposições legislativas por deputado");
		//
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//
		job.setMapperClass(ProposicoesPorEstadoMapper.class);
		job.setReducerClass(ProposicoesPorEstadoReducer.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
