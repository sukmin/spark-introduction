package me.ujung.spark;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author sukmin.kwon
 * @since 2017-01-17
 */
public class DriverTest {

	@Test
	public void testSplit(){
		// given
		String test = "a  b";

		// when
		String [] result = test.split(" ");

		// then
		Assert.assertEquals(3,result.length);
	}

}
