package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MunicipiosBeneficiadosMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text chave = new Text();
    private IntWritable valor = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String valores[] = value.toString().split("\t");
	String uf = valores[0];
	// Verifica se a linha não é de cabeçalho
	if ("UF".equals(uf)) {
	    return;
	}
	String municipio = valores[2];
	String strValor = valores[10].replaceAll("\\.00", "").replaceAll(",", "");
	Integer iValor = 0;
	try {
	    iValor = Integer.parseInt(strValor);
	} catch (NumberFormatException e) {
	    e.printStackTrace();
	}
	valor.set(iValor);
	chave.set(uf + "-" + municipio);
	context.write(chave, valor);
    }
}
