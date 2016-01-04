package net.marcoreis.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ChamTtcTermsWritable implements WritableComparable<ChamTtcTermsWritable> {
	private int cham;
	private int ttc;
	private int imeis;

	public ChamTtcTermsWritable() {
		set(cham, ttc, imeis);
	}

	/**
	 * Use o formato cham,ttc,count(imeis)
	 * 
	 * @param valor
	 */
	public void set(String valor) {
		String[] dados = valor.split(",");
		this.cham = Integer.parseInt(dados[0]);
		this.ttc = Integer.parseInt(dados[1]);
		this.imeis = Integer.parseInt(dados[2]);
	}

	public void set(int cham, int ttc, int imeis) {
		this.cham = cham;
		this.ttc = ttc;
		this.imeis = imeis;
	}

	public int getCham() {
		return cham;
	}

	public int getTtc() {
		return ttc;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(cham);
		out.writeInt(ttc);
		out.writeInt(imeis);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cham = in.readInt();
		ttc = in.readInt();
		imeis = in.readInt();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = cham;
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = ttc;
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = imeis;
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof ChamTtcTermsWritable)) {
			return false;
		}
		ChamTtcTermsWritable other = (ChamTtcTermsWritable) obj;
		if (cham != other.cham) {
			return false;
		}
		if (ttc != other.ttc) {
			return false;
		}
		if (imeis != other.imeis) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return cham + "," + ttc + "," + imeis;
	}

	public int getImeis() {
		return imeis;
	}

	public void setImeis(int imeis) {
		this.imeis = imeis;
	}

	@Override
	public int compareTo(ChamTtcTermsWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}