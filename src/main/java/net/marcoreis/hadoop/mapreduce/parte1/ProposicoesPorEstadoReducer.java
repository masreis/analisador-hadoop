package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProposicoesPorEstadoReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
	private Text chave = new Text();

	protected void reduce(Text chaveMap, Iterable<LongWritable> valores,
			Reducer<Text, LongWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		long totalDeProposicoes = 0;
		for (LongWritable valor : valores) {
			totalDeProposicoes = totalDeProposicoes + valor.get();
		}
		// Separador padrão é TAB
		String strChave = chaveMap + "\t" + totalDeProposicoes;
		chave.set(strChave);
		context.write(chave, NullWritable.get());
	}
}
