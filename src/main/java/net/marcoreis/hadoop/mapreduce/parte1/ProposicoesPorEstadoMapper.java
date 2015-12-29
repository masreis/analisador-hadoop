package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProposicoesPorEstadoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] valores = value.toString().split(";");
		String estado = valores[36]; // Nome civil do deputado
		// Verifica se o registro é válido e ignora a primeira linha
		if (!NumberUtils.isNumber(valores[2]) || "N/A".equals(estado)) {
			return;
		}
		context.write(new Text(estado), new IntWritable(1));
	}
}
