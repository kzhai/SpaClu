package com.yahoo.spaclu.data.structure;

import java.util.LinkedList;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.jblas.util.Random;
import org.junit.Test;

public class TriplesAndTripleOfInts {
	// public static double PRECISION = 1e-12;
	public static int CAPACITY = 1000000;

	@Test
	public void testTimeConstructor() {
		long timeTriples = System.currentTimeMillis();
		for (int i = 0; i < CAPACITY; i++) {
			Triples<Integer, Integer, Integer> triples = new Triples<Integer, Integer, Integer>(
					Random.nextInt(Integer.MAX_VALUE),
					Random.nextInt(Integer.MAX_VALUE),
					Random.nextInt(Integer.MAX_VALUE));
		}
		System.out
				.println("Create " + CAPACITY + " " + Triples.class.getName()
						+ " takes "
						+ (System.currentTimeMillis() - timeTriples) + "ms");

		long timeTripleOfInts = System.currentTimeMillis();
		for (int i = 0; i < CAPACITY; i++) {
			TripleOfInts tripleOfInts = new TripleOfInts(
					Random.nextInt(Integer.MAX_VALUE),
					Random.nextInt(Integer.MAX_VALUE),
					Random.nextInt(Integer.MAX_VALUE));
		}
		System.out.println("Create " + CAPACITY + " "
				+ TripleOfInts.class.getName() + " takes "
				+ (System.currentTimeMillis() - timeTripleOfInts) + "ms");
	}

	@Test
	public void testTimeSerialization() {
		List<Triples> listOfTriples = new LinkedList<Triples>();
		List<TripleOfInts> listOfTripleOfInts = new LinkedList<TripleOfInts>();
		for (int i = 0; i < CAPACITY; i++) {
			listOfTriples.add(new Triples<Integer, Integer, Integer>(Random
					.nextInt(Integer.MAX_VALUE), Random
					.nextInt(Integer.MAX_VALUE), Random
					.nextInt(Integer.MAX_VALUE)));

			listOfTripleOfInts.add(new TripleOfInts(Random
					.nextInt(Integer.MAX_VALUE), Random
					.nextInt(Integer.MAX_VALUE), Random
					.nextInt(Integer.MAX_VALUE)));
		}

		long timeTriples = System.currentTimeMillis();
		for (int i = 0; i < CAPACITY; i++) {
			listOfTriples.get(i).toString();

		}
		System.out
				.println("Create " + CAPACITY + " " + Triples.class.getName()
						+ " takes "
						+ (System.currentTimeMillis() - timeTriples) + "ms");

		long timeTripleOfInts = System.currentTimeMillis();
		for (int i = 0; i < CAPACITY; i++) {
			listOfTripleOfInts.get(i).toString();
		}
		System.out.println("Create " + CAPACITY + " "
				+ TripleOfInts.class.getName() + " takes "
				+ (System.currentTimeMillis() - timeTripleOfInts) + "ms");
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TriplesAndTripleOfInts.class);
	}
}