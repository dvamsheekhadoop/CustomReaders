package com.jpmc.parsers;

public class BinaryObj {
	private int id;
	private String description;
	private double value;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public BinaryObj(int id, double value, String description) {
		this.id = id;
		this.value = value;
		this.description = description;
	}

	@Override
	public String toString() {
		return this.id + "\t" + this.value + "\t" + this.description;
	}
}
