package net.marcoreis.analisador.teste;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class GrepSeekable {
    private Map<String, Integer> mapa = new HashMap<String, Integer>();

    public static void main(String[] args) {
	new GrepSeekable().analisar(args[0], args[1]);
    }

    private void analisar(String arquivo, String padrao) {
	try {
	    try (SeekableByteChannel ch = Files.newByteChannel(Paths.get(arquivo))) {
		ByteBuffer bb = ByteBuffer.allocateDirect(1000);
		for (;;) {
		    StringBuilder line = new StringBuilder();
		    int n = ch.read(bb);
		    line.append(n);
		    // add chars to line
		    // ...
		}
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
