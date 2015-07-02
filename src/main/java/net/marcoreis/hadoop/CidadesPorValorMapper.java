package net.marcoreis.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CidadesPorValorMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

}
