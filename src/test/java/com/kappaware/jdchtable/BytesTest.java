package com.kappaware.jdchtable;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class BytesTest {
	
	
	
	public void test(String s, byte[] ba, String expected) {
		byte[] r = Bytes.toBytesBinary(s);
		String s2 = Bytes.toStringBinary(r);
		//System.out.println(s2);
		Assert.assertArrayEquals(ba, r);
		Assert.assertEquals(expected, s2);
	}
	
	
	
	@Test
	public void test1() {
		test("abcd", new byte[] { 'a', 'b', 'c', 'd'}, "abcd");
		test("\\x00", new byte[] { 0x00 }, "\\x00");
		test("\\x0D", new byte[] { 0x0D }, "\\x0D");
		test("\\x00-\\x0D+", new byte[] { 0x00, '-', 0x0d, '+' }, "\\x00-\\x0D+");
	}
	
	@Test
	public void splitTest() {
		String first = "\\x00";
		String last =  "\\xFF";
		byte[][] r = Bytes.split(Bytes.toBytesBinary(first), Bytes.toBytesBinary(last), true, 4);
		for(int i = 0; i < r.length; i++) {
			String x = Bytes.toStringBinary(r[i]);
			System.out.println(x);
		}
		byte[][] r2 = Arrays.copyOfRange(r, 1, 5);
		System.out.println("-------------------------");
		for(int i = 0; i < r2.length; i++) {
			String x = Bytes.toStringBinary(r2[i]);
			System.out.println(x);
		}
	}

	//@Test
	public void splitTest2() {
		String first = "\\x00\\x00";
		String last =  "\\xFF\\xFF";
		byte[][] r = Bytes.split(Bytes.toBytesBinary(first), Bytes.toBytesBinary(last), true, 4);
		for(int i = 0; i < r.length; i++) {
			String x = Bytes.toStringBinary(r[i]);
			System.out.println(x);
		}
		
	}
}
