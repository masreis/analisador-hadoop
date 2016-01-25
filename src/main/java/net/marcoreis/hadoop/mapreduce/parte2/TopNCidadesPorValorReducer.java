package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNCidadesPorValorReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    private Map<Text, IntWritable> mapa = new HashMap<Text, IntWritable>();

    @Override
    protected void reduce(LongWritable chave, Iterable<Text> valores, Context context)
	    throws IOException, InterruptedException {
	//
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
	int quantidade = 10;
	Map<Text, IntWritable> mapaOrdenado = ordenarMapa(mapa);
	for (Text chave : mapaOrdenado.keySet()) {
	    if (quantidade == 10) {
		break;
	    }
	    IntWritable total = mapaOrdenado.get(chave);
	    // context.write(chave, new IntWritable(total));
	}
    }

    private Map<Text, IntWritable> ordenarMapa(Map<Text, IntWritable> mapa) {
	return null;
    }
}
