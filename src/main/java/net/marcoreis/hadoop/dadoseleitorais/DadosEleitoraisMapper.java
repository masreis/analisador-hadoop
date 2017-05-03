package net.marcoreis.hadoop.dadoseleitorais;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DadosEleitoraisMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text chave = new Text();
	private DoubleWritable valor = new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {

		// "C<F3>d. Elei<E7><E3>o";"Desc. Elei<E7><E3>o";"Data e hora";"CNPJ
		// Prestador Conta";"Sequencial Candidato";"UF";"Sigla da UE";"Nome da
		// UE";"Sigla Partido";"N<FA>mero candidato";"Cargo";"Nome
		// candidato";"CPF do candidato";"CPF do vice/suplente";"Tipo de
		// documento";"N<FA>mero do documento";"CPF/CNPJ do fornecedor";"Nome do
		// fornecedor";"Nome do fornecedor (Receita Federal)";"Cod setor
		// econ<F4>mico do fornecedor";"Setor econ<F4>mico do fornecedor";"Data
		// da despesa";"Valor despesa";"Tipo despesa";"Descri<E7>ao da despesa"
		// String conteudo = value.toString().replaceAll("\"", "");
		String valores[] = value.toString().split("\";\"");
		int idValor = 22;
		int idData = 21;
		String tipo = "despesa";
		if (valores.length == 35) {
			tipo = "receita";
			idValor = 25;
			idData = 24;
		} else if (valores.length == 21) {
			idValor = 18;
			idData = 17;
			tipo = "despesa";
		}
		String uf = valores[5];
		if ("UF".equals(uf)) {
			return;
		}
		String municipio = valores[7];
		try {
			String strValor = valores[idValor];
			Double dValor = Double
					.parseDouble(strValor.replaceAll(",", "."));
			valor.set(dValor);
			String[] data = valores[idData].split("/");
			String ano = data[2].substring(0, 4);
			chave.set(uf + ";" + municipio + ";" + ano + ";"
					+ tipo);
			context.write(chave, valor);
		} catch (Exception e) {
			System.out.println("Erro------------------------");
		}
	}
}
