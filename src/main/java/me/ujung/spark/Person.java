package me.ujung.spark;

import java.io.Serializable;

/**
 * @author sukmin.kwon
 * @since 2017-03-08
 */
public class Person implements Serializable {

	private double height;
	private double weight;
	private int age;
	private double label;

	public Person() {

	}

	public Person(double height, double weight, int age, double label) {

		this.height = height;
		this.weight = weight;
		this.age = age;
		this.label = label;
	}

	public double getHeight() {
		return height;
	}

	public void setHeight(double height) {
		this.height = height;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public double getLabel() {
		return label;
	}

	public void setLabel(double label) {
		this.label = label;
	}
}
