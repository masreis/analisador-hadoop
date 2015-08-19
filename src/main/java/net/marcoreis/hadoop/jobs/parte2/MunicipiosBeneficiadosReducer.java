package net.marcoreis.hadoop.jobs.parte2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MunicipiosBeneficiadosReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void reduce(LongWritable chave, Iterable<Text> valores, Context context)
			throws IOException, InterruptedException {
		//
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}

}
