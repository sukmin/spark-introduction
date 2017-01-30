package me.ujung.spark;

import java.io.Serializable;

/**
 * @author sukmin.kwon
 * @since 2017-01-21
 */
public class Name implements Serializable {

	private String source;

	public Name(String source) {
		this.source = source;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Name name = (Name)o;

		return source.equals(name.source);
	}

	@Override
	public int hashCode() {
		return source.hashCode();
	}

	@Override
	public String toString() {
		return "Name{" +
			"source='" + source + '\'' +
			'}';
	}
}
