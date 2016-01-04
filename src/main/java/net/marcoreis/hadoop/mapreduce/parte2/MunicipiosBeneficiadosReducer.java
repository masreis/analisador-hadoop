package net.marcoreis.hadoop.mapreduce.parte2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MunicipiosBeneficiadosReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private DoubleWritable totalPorMunicipio = new DoubleWritable();

	@Override
	protected void reduce(Text chave, Iterable<DoubleWritable> valores,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context contexto)
					throws IOException, InterruptedException {
		double total = 0;
		for (DoubleWritable valor : valores) {
			total = total + valor.get();
		}
		totalPorMunicipio.set(total);
		contexto.write(chave, totalPorMunicipio);
	}
}
