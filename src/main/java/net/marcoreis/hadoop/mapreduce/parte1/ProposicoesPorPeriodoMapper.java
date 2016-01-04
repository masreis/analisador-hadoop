package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProposicoesPorPeriodoMapper extends Mapper<LongWritable, Text, ShortWritable, LongWritable> {
	private ShortWritable chave = new ShortWritable();
	private LongWritable valor = new LongWritable(1);

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] valores = value.toString().split(";");
		String data = valores[0];
		String ano = data.substring(6, 10);

		// Verifica se o período é numérico
		if (!NumberUtils.isDigits(ano.substring(0, 1))) {
			return;
		}
		chave.set(Short.parseShort(ano));
		context.write(chave, valor);
	}
}
