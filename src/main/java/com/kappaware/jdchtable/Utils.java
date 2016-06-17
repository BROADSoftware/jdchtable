package com.kappaware.jdchtable;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;


public class Utils {
	static public final String DEFAULT_NS = "default";

	public static <T extends Enum<T>> T parseEnum(Class<T> clazz, Object value, String property, T defaultValue) throws DescriptionException {
		if (value == null) {
			return defaultValue;
		} else {
			try {
				return Enum.valueOf(clazz, value.toString());
			} catch (Exception e) {
				EnumSet<T> set = EnumSet.allOf(clazz);
				throw new DescriptionException(String.format("Invalid value '%s' for property '%s'. Must be one of %s", value, property, set.toString()));
			}
		}
	}
	
	private static HashSet<String> trueValues = new HashSet<String>(Arrays.asList("yes", "true", "1", "si", "oui", "ya"));
	private static HashSet<String> falseValues = new HashSet<String>(Arrays.asList("no", "false", "0", "no", "non", "nein"));

	static public boolean parseBoolean(String s, Boolean valueOnNull, String propertyName) throws DescriptionException {
		if (s == null || s.length() == 0) {
			return valueOnNull;
		} else {
			s = s.toLowerCase();
			if (trueValues.contains(s)) {
				return true;
			} else if (falseValues.contains(s)) {
				return false;
			} else {
				throw new DescriptionException(String.format("Invalid boolean value '%s' for property '%s'!", s, propertyName));
			}
		}
	}

	
	
}
