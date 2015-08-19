package net.marcoreis.hadoop.jobs.parte2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MunicipiosBeneficiadosMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valores[] = value.toString().split("\t");
		String uf = valores[0];
		String municipio = valores[3];
		String strValor = valores[10];
		Double valor = Double.parseDouble(strValor);
		Text chave = new Text(uf + "-" + municipio);
		context.write(chave, new DoubleWritable(valor));
	}
}
