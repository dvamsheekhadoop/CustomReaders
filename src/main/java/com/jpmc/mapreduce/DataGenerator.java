package com.jpmc.mapreduce;

import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;

public class DataGenerator {

	private static final char[] LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			.toCharArray();
	private static final char[] ALPHANUMERICS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
			.toCharArray();
	private static final char[] NUMBERS = "0123456789".toCharArray();

	private SecureRandom rand;

	public DataGenerator(SecureRandom rand) {
		this.rand = rand;
	}

	public DataGenerator() {
		this.rand = new SecureRandom(new Date().toString().getBytes());
	}

	public String fixedLengthString(int len) {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < len; i++) {
			char c = ALPHANUMERICS[rand.nextInt(ALPHANUMERICS.length)];
			buf.append(c);
		}
		return buf.toString();
	}

	public String ficedLengthNumString(int len) {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < len; i++) {
			char c = NUMBERS[rand.nextInt(LETTERS.length)];
			buf.append(c);
		}
		return buf.toString();
	}

	public char randomLetterChar() {
		return LETTERS[rand.nextInt(LETTERS.length)];
	}

	public double randomDouble() {
		return rand.nextDouble();
	}

	public double randomPositiveDouble() {
		return Math.abs(rand.nextDouble());
	}

	public int randomInt(int max) {
		return rand.nextInt();
	}

	public int randomPositiveInt(int max) {
		return Math.abs(rand.nextInt(max));
	}

	public int randomInt() {
		return rand.nextInt();
	}

	public int randomPositiveInt() {
		return Math.abs(rand.nextInt());
	}

	public long randomPositiveLong() {
		long l = rand.nextLong();
		if (l < 0)
			l = l * -1;
		return l;
	}

	public long randomLong() {
		return rand.nextLong();
	}

	public String randomString(int minLen, int maxLen) {
		int diff = maxLen - minLen;
		int len = minLen + rand.nextInt(diff);
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < len; i++) {
			char c = ALPHANUMERICS[rand.nextInt(ALPHANUMERICS.length)];
			buf.append(c);
		}
		return buf.toString();
	}

	public String randomNumString(int minLen, int maxLen) {
		int diff = maxLen - minLen;
		int len = minLen + rand.nextInt(diff);
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < len; i++) {
			char c = NUMBERS[rand.nextInt(NUMBERS.length)];
			buf.append(c);
		}
		return buf.toString();
	}

	public Date ranDate(int baseYear, int baseMonth, int baseDay) {

		int y = baseYear + rand.nextInt(40);
		int m = baseMonth + rand.nextInt(12);
		int d = baseDay + rand.nextInt(30);

		Calendar cal = Calendar.getInstance();
		cal.set(y, m, d);
		return cal.getTime();
	}

	public Calendar randomCalendar(int baseYear, int baseMonth, int baseDay) {

		int y = baseYear + rand.nextInt(40);
		int m = baseMonth + rand.nextInt(12);
		int d = baseDay + rand.nextInt(30);

		Calendar cal = Calendar.getInstance();
		cal.set(y, m, d);
		return cal;
	}
}
