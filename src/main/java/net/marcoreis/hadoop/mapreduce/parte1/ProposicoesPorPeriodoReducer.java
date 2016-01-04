package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProposicoesPorPeriodoReducer extends Reducer<ShortWritable, LongWritable, IntWritable, IntWritable> {
	private IntWritable chave = new IntWritable();
	private IntWritable valor = new IntWritable();

	protected void reduce(ShortWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int totalDeProposicoes = 0;
		for (LongWritable valor : values) {
			totalDeProposicoes = totalDeProposicoes + Integer.parseInt(valor.toString());
		}
		valor.set(totalDeProposicoes);
		chave.set(Integer.parseInt(key.toString()));
		context.write(chave, valor);
	}
}
