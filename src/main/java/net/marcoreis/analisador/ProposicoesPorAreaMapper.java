package net.marcoreis.analisador;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProposicoesPorAreaMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] valores = value.toString().split(";");
		String ano = valores[0].substring(6);
		String area = valores[32]; // Area da proposicao
		context.write(new Text(ano + "-" + area), new IntWritable(1));
	}
}
