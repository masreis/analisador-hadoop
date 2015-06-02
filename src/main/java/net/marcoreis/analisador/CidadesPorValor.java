package net.marcoreis.analisador;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.junit.Assert;

public class CidadesPorValor extends Configured implements Tool {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.addResource("configuracao-bolsa-familia.xml");
		Assert.assertThat(conf.get("quantidade.limite.cidades"), );
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
