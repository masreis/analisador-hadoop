package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProposicoesPorAreaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {

	int totalDeProposicoes = 0;
	for (IntWritable value : values) {
	    totalDeProposicoes++;
	}
	context.write(key, new IntWritable(totalDeProposicoes));
    }
}
