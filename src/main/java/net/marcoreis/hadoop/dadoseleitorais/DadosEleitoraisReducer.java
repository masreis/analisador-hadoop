package net.marcoreis.hadoop.dadoseleitorais;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DadosEleitoraisReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key,
			Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		Double valorTotal = 0d;
		for (DoubleWritable value : values) {
			valorTotal += value.get();
		}
		context.write(key, new DoubleWritable(valorTotal));
	}
}
