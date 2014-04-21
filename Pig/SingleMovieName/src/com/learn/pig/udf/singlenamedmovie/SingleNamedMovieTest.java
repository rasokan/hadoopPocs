package com.learn.pig.udf.singlenamedmovie;

import java.io.IOException;


public class SingleNamedMovieTest {

	private final static String TAB_SPACE = " ";

	public static void main(String a[]) throws IOException {

		SingleNamedMovieVerify singleNamedMovie = new SingleNamedMovieVerify();

		System.out.println(singleNamedMovie.execString("Toy Story (1995)"));
		
		System.out.println(singleNamedMovie.execString("Copycat (1995)"));

	}

}
