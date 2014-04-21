package com.hive.udf.isnumeric;

import org.apache.hadoop.hive.ql.exec.UDF;

public class IsNumeric extends UDF {

	public String evaluate(String input) {

		int count = 0;

		if (input != null) {

			String inputArray[] = input.split(" ");
			for (int i = 0; i < inputArray.length - 1; i++) {

				count = 0;

				try {
					long value = Long.parseLong(inputArray[i]);
					count++;
				} catch (Exception e) {

				}
			}
			if (count == inputArray.length - 1) {
				return input;
			}
			return "";
		} else {
			return "";
		}
	}

}
