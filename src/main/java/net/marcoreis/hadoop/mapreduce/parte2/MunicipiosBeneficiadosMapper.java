package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MunicipiosBeneficiadosMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text chave = new Text();
    private DoubleWritable valor = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String valores[] = value.toString().split("\t");
	String uf = valores[0];
	// Verifica se a linha não é de cabeçalho
	if ("UF".equals(uf)) {
	    return;
	}
	String municipio = valores[2];
	String strValor = valores[10];
	Double dValor = Double.parseDouble(strValor.replaceAll(",", ""));
	valor.set(dValor);
	chave.set(uf + "-" + municipio);
	context.write(chave, valor);
    }
}
