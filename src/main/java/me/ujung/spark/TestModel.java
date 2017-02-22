package me.ujung.spark;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author sukmin.kwon
 * @since 2017-02-22
 */
public class TestModel implements Serializable {

	private long id;
	private String name;
	private int score;
	private String dateOfRegistration;

	public TestModel(Long id, String name, int score) {
		this.id = id;
		this.name = name;
		this.score = score;
		this.dateOfRegistration = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public int getScore() {
		return score;
	}

	public String getDateOfRegistration() {
		return dateOfRegistration;
	}

	@Override
	public String toString() {
		return "TestModel{" +
			"id=" + id +
			", name='" + name + '\'' +
			", score=" + score +
			", dateOfRegistration='" + dateOfRegistration + '\'' +
			'}';
	}
}
