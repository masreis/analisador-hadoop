package net.marcoreis.hadoop.jobs.parte1;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProposicoesPorPeriodoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] valores = value.toString().split(";");
		String data = valores[0];
		// String periodo = data.substring(6, 10); // Ano
		String periodo = data.substring(3, 10); // Mês-Ano

		// Verifica se o período é numérico
		if (!NumberUtils.isDigits(periodo.substring(0, 1))) {
			return;
		}
		context.write(new Text(periodo), new IntWritable(1));
	}
}
