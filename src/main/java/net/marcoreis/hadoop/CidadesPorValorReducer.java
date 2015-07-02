package net.marcoreis.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CidadesPorValorReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

}
