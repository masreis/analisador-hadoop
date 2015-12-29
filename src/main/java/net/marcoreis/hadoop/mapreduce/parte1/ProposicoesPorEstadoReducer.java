package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.marcoreis.hadoop.util.Comparador;

public class ProposicoesPorEstadoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private Map<String, Integer> mapaEstados = new HashMap<String, Integer>();
	private int totalDeProposicoesBrasil;

	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		//
		String estado = key.toString();
		//
		int totalDePreposicoesDoEstado = 0;
		for (IntWritable valor : values) {
			totalDePreposicoesDoEstado += valor.get();
			totalDeProposicoesBrasil += valor.get();
		}
		mapaEstados.put(estado, totalDePreposicoesDoEstado);
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//
		Comparador comparador = new Comparador(mapaEstados);
		TreeMap<String, Integer> mapaOrdenado = new TreeMap<String, Integer>(comparador);
		mapaOrdenado.putAll(mapaEstados);
		//
		int contador = 0;
		for (String estado : mapaOrdenado.keySet()) {
			if (contador == 3) {
				break;
			}
			Integer totalDePreposicoesDoEstado = mapaOrdenado.get(estado);
			context.write(new Text(estado), new IntWritable(totalDePreposicoesDoEstado));
			contador++;
		}
	}
}
