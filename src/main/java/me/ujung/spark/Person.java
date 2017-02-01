package me.ujung.spark;

import java.io.Serializable;

/**
 * @author sukmin.kwon
 * @since 2017-02-01
 */
public class Person implements Serializable {

	private long id;
	private String name;
	private int age;
	private String address;

	public Person() {

	}

	public Person(int id, String name, int age, String address) {
		this.id = id;
		this.name = name;
		this.age = age;
		this.address = address;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	@Override
	public String toString() {
		return "Person{" +
			"id=" + id +
			", name='" + name + '\'' +
			", age=" + age +
			", address='" + address + '\'' +
			'}';
	}
}
