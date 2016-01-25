package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaiorValorDeProposicoesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private Text chave = new Text();
    private LongWritable valor = new LongWritable();
    private Map<String, Long> mapaParlamentares = new HashMap<String, Long>();

    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
	    throws IOException, InterruptedException {
	long totalDeProposicoes = 0;
	for (LongWritable valor : values) {
	    totalDeProposicoes = totalDeProposicoes + valor.get();
	}
	String codigoParlamentar = key.toString();
	mapaParlamentares.put(codigoParlamentar, totalDeProposicoes);
    }

    @Override
    protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
	    throws IOException, InterruptedException {
	Set<String> chaves = mapaParlamentares.keySet();
	String codigoParlamentarMaiorValor = "";
	Long maiorValor = 0l;
	for (String codigoParlamentar : chaves) {
	    Long quantidadeParlamentar = mapaParlamentares.get(codigoParlamentar);
	    if (quantidadeParlamentar > maiorValor) {
		codigoParlamentarMaiorValor = codigoParlamentar;
		maiorValor = quantidadeParlamentar;
	    }
	}
	chave.set(codigoParlamentarMaiorValor);
	valor.set(maiorValor);
	context.write(chave, valor);
    }
}
