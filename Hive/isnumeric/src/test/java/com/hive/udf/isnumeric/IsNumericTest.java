package com.hive.udf.isnumeric;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IsNumericTest {

	@Test
	public void testValue() {
		IsNumeric isNumeric = new IsNumeric();
		Assert.assertEquals("12345", isNumeric.evaluate("12345"));
		
		Assert.assertEquals("", isNumeric.evaluate("skyfall"));
		
		Assert.assertEquals("", isNumeric.evaluate("skyfall is falling"));
	}
}
