package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProposicoesPorEstadoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private Text chave = new Text();
	private LongWritable valor = new LongWritable(1);

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] valores = value.toString().split(";");
		String estado = valores[36]; // UF
		// Verifica se o registro é válido
		if (!NumberUtils.isNumber(valores[2]) || "N/A".equals(estado)) {
			return;
		}
		chave.set(estado);
		context.write(chave, valor);
	}
}
