package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalPorMunicipioReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalPorMunicipio = new IntWritable();

    @Override
    protected void reduce(Text chave, Iterable<IntWritable> valores,
	    Reducer<Text, IntWritable, Text, IntWritable>.Context contexto) throws IOException, InterruptedException {
	int total = 0;
	for (IntWritable valor : valores) {
	    total = total + valor.get();
	}
	totalPorMunicipio.set(total);
	contexto.write(chave, totalPorMunicipio);
    }
}
