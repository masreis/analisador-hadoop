package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaiorValorDeProposicoesMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text chave;
    private LongWritable valor;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
	    throws IOException, InterruptedException {
	chave = new Text();
	valor = new LongWritable(1);
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] valores = value.toString().split(";");
	String data = valores[0];
	String ano = data.substring(6, 10);
	String codigoParlamentar = valores[7];
	// ou String nomeParlamentar = valores[4];
	String uf = valores[36];
	// Verifica se o período é numérico
	if (!NumberUtils.isDigits(ano.substring(0, 1)) || "N/A".equals(uf)) {
	    return;
	}
	chave.set(uf + ";" + codigoParlamentar);
	context.write(chave, valor);
    }
}
