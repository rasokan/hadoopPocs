package com.learn.hadoop.mapreduce.olympicathletespoc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class TestClass2 {

	@Test
	public void testValue() {

		StringBuffer bufferValue = new StringBuffer();

		bufferValue.append("aaa").append("bbb").append("ccc");

		int len = bufferValue.length();

		System.out.println(len);

		bufferValue.replace(0, bufferValue.length(), "");

		System.out.println(bufferValue);

		System.out.println(bufferValue.length());

		List<Integer> intList = new ArrayList<Integer>();

		List<Integer> intTempList = new ArrayList<Integer>();

		intList.add(1234);

		intList.add(1234);

		intList.add(1234);

		for (Integer value : intList) {

			intTempList.add(345);
		}

		intList.add(2345);

		intList.add(12);

		Set<String> set = new HashSet<String>();

		set.add("Hola");

		set.add("Hola");

		for (String value : set) {

			set.add("hi");

			System.out.println(value);

		}

		for (String value : set) {

			System.out.println(value);

		}

	}
}
