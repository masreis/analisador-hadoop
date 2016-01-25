package net.marcoreis.hadoop.util;

import java.util.Comparator;
import java.util.Map;

public class Comparador implements Comparator<String> {

    private Map<String, Integer> map;

    public Comparador(Map<String, Integer> base) {
	this.map = base;
    }

    public int compare(String a, String b) {
	if (map.get(a) > map.get(b)) {
	    return -1;
	} else {
	    return 1;
	}
    }
}