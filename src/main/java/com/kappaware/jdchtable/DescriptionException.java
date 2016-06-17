package com.kappaware.jdchtable;

@SuppressWarnings("serial")
public class DescriptionException extends Exception {

	public DescriptionException(String m) {
		super(m);
	}

	public DescriptionException(String m, Exception e) {
		super(m, e);
	}

}
