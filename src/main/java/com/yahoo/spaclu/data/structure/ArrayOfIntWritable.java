package com.yahoo.spaclu.data.structure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * An array of ints that implements Writable class.
 * 
 * @author Ke Zhai
 */
public class ArrayOfIntWritable implements Writable {
	int[] array;

	/**
	 * Constructor with no arguments.
	 */
	public ArrayOfIntWritable() {
		super();
	}

	/**
	 * Constructor take in a one-dimensional array.
	 * 
	 * @param array
	 *            input int array
	 */
	public ArrayOfIntWritable(int[] array) {
		this.array = array;
	}

	/**
	 * Constructor that takes the size of the array as an argument.
	 * 
	 * @param size
	 *            number of ints in array
	 */
	public ArrayOfIntWritable(int size) {
		super();
		array = new int[size];
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		array = new int[size];
		for (int i = 0; i < size; i++) {
			set(i, in.readInt());
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for (int i = 0; i < size(); i++) {
			out.writeInt(get(i));
		}
	}

	/**
	 * Get a deep copy of the array.
	 * 
	 * @return a clone of the array
	 */
	public int[] getClone() {
		return array.clone();
	}

	/**
	 * Get a shallow copy of the array.
	 * 
	 * @return a pointer to the array
	 */
	public int[] getArray() {
		return array;
	}

	/**
	 * Set the array.
	 * 
	 * @param array
	 */
	public void setArray(int[] array) {
		this.array = array;
	}

	/**
	 * Returns the int value at position <i>i</i>.
	 * 
	 * @param i
	 *            index of int to be returned
	 * @return int value at position <i>i</i>
	 */
	public int get(int i) {
		return array[i];
	}

	/**
	 * Sets the int at position <i>i</i> to <i>f</i>.
	 * 
	 * @param i
	 *            position in array
	 * @param f
	 *            int value to be set
	 */
	public void set(int i, int f) {
		array[i] = f;
	}

	/**
	 * Returns the size of the int array.
	 * 
	 * @return size of array
	 */
	public int size() {
		return array.length;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size(); i++) {
			sb.append(get(i));
			sb.append(" ");
		}
		return sb.toString().trim();
	}
}