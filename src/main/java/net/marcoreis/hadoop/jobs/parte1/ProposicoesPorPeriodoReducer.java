package net.marcoreis.hadoop.jobs.parte1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProposicoesPorPeriodoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable valor = new IntWritable();

	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int totalDeProposicoes = 0;
		for (IntWritable valor : values) {
			totalDeProposicoes += valor.get();
		}
		valor.set(totalDeProposicoes);
		context.write(key, valor);
	}
}
