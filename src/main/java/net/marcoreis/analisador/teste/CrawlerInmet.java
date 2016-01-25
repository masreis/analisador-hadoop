package net.marcoreis.analisador.teste;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CrawlerInmet {

    public static void main(String[] args) {
	try {
	    DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	    String data = df.format(new Date());
	    String url = "http://www.inmet.gov.br/projetos/rede/pesquisa/gera_serie_txt.php?&mRelEstacao=83377&btnProcesso=serie&mRelDtInicio=01/01/1960&mRelDtFim="
		    + data + "&mAtributos=1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1";
	    String html = null;// Jsoup.connect(url).get().html();
	    System.out.println(html);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private void recuperarEstacoes() {

    }

}
