package net.marcoreis.hadoop.mapreduce.parte1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProposicoesPorAreaMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text chave = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] valores = value.toString().split(";");
	String areasComVirgula = valores[32]; // Area da proposicao
	String[] areas = areasComVirgula.split(",");
	for (String area : areas) {
	    context.write(chave, NullWritable.get());
	}
    }
}
