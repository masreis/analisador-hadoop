package net.marcoreis.hadoop.mapreduce.parte1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProposicoesPorPeriodoDriverV2 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ProposicoesPorPeriodoDriverV2(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		//
		job.setJarByClass(ProposicoesPorPeriodoDriverV2.class);
		job.setJobName("Contador de proposições legislativas por período");
		//
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//
		job.setMapperClass(ProposicoesPorPeriodoMapper.class);
		job.setReducerClass(ProposicoesPorPeriodoReducer.class);
		//
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
