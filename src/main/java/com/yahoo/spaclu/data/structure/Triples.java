package com.yahoo.spaclu.data.structure;

import java.io.Serializable;

public class Triples<S1, S2, S3> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8227342166873157809L;

	private S1 first;
	private S2 second;
	private S3 third;

	public Triples(S1 first, S2 second, S3 third) {
		this.setFirst(first);
		this.setSecond(second);
		this.setThird(third);
	}

	public S1 getFirst() {
		return first;
	}

	public void setFirst(S1 first) {
		this.first = first;
	}

	public S2 getSecond() {
		return second;
	}

	public void setSecond(S2 second) {
		this.second = second;
	}

	public S3 getThird() {
		return third;
	}

	public void setThird(S3 third) {
		this.third = third;
	}

	public String toString() {
		return first + ":" + second + ":" + third;
	}

	public boolean equalsTo(Object o) {
		if (!(o instanceof Triples))
			return false;
		Triples<?, ?, ?> e = (Triples<?, ?, ?>) o;
		return first.equals(e.getFirst()) && second.equals(e.getSecond())
				&& third.equals(e.getThird());
	}
}