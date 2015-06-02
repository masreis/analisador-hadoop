package net.marcoreis.analisador.teste;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Grep {
	private Map<String, Integer> mapa = new HashMap<String, Integer>();
	private static Logger logger = Logger.getLogger(Grep.class.getName());

	public static void main(String[] args) {
		logger.info("Inicio");
		new Grep().analisar(args[0], args[1]);
		logger.info("Fim");
	}

	private void analisar(String arquivo, String padrao) {
		try {
			FileInputStream fstream = new FileInputStream(arquivo);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fstream));
			int i = 0;
			String line;
			Pattern pattern = Pattern.compile(padrao);
			while ((line = br.readLine()) != null) {
				i++;
				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					String group = matcher.group();
					Integer total = 1;
					if (mapa.get(group) != null) {
						total = 1 + mapa.get(group);
					}
					mapa.put(group, total);
				}
			}
			//
			br.close();
			logger.info(mapa.toString());
			System.out.println(i);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
