package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalPorMunicipioMapper
		extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text chave = new Text();
	private IntWritable valor = new IntWritable();
	private boolean incluirData;

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		incluirData = context.getConfiguration()
				.get("incluir.data") != null;
	}

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String valores[] = value.toString().split("\t");
		String uf = valores[0];
		// Verifica se a linha não é de cabeçalho
		if ("UF".equals(uf)) {
			return;
		}
		String municipio = valores[2];
		String strValor = valores[10].replaceAll("\\.00", "")
				.replaceAll(",", "");
		Integer iValor = Integer.parseInt(strValor);
		valor.set(iValor);
		if (incluirData) {
			String[] data = valores[11].split("/");
			String ano = data[1];
			String mes = data[0];
			chave.set(uf + "\t" + municipio + "\t" + ano + "\t"
					+ mes);
		} else {
			chave.set(uf + "\t" + municipio);
		}
		context.write(chave, valor);
	}
}
