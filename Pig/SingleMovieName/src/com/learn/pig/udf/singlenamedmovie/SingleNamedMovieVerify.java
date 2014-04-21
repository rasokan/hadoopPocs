package com.learn.pig.udf.singlenamedmovie;

import java.io.IOException;

public class SingleNamedMovieVerify {
	
	private final static String TAB_SPACE = " ";
	
	public Boolean execString(String movieName) throws IOException {
		// TODO Auto-generated method stub

		Boolean result = false;

		if (movieName != null) {

			String movieNameBeforeSplit = (String) movieName;

			String movieNameAfterSplit[] = movieNameBeforeSplit
					.split(TAB_SPACE);

			if (movieNameAfterSplit.length <= 2) {

				result = true;
			}

		}
		return result;
	}
	
	

}
