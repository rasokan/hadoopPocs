package com.learn.pig.udf.singlenamedmovie;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class SingleNamedMovie extends FilterFunc {

	private final static String TAB_SPACE = " ";

	@Override
	public Boolean exec(Tuple movieName) throws IOException {
		// TODO Auto-generated method stub

		Boolean result = false;

		if (movieName != null) {

			String movieNameBeforeSplit = (String) movieName.get(0);

			String movieNameAfterSplit[] = movieNameBeforeSplit
					.split(TAB_SPACE);

			if (movieNameAfterSplit.length <= 2) {

				result = true;
			}

		}
		return result;
	}

}
