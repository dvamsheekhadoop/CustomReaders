package com.jpmc.mapreduce;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SampleDataService {

	public SampleDataService() {

	}

	private static class Test {
		private int id;
		private String description;
		private double value;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public double getValue() {
			return value;
		}

		public void setValue(double value) {
			this.value = value;
		}

		private Test(int id, double value, String description) {
			super();
			this.id = id;
			this.value = value;
			this.description = description;
		}

	}

	public static void main(String[] args) throws Exception {

		FileOutputStream fout = new FileOutputStream(
				"src/test/resources/sample.dat");
		DataGenerator dg = new DataGenerator();

		try {
			DataOutputStream dout = new DataOutputStream(fout);
			String header = "1234567890";
			String footer = "1234567890";
			dout.write(header.getBytes());
			for (int i = 0; i < 10000000; i++) {
				Test t = new Test(dg.randomPositiveInt(),
						dg.randomPositiveDouble(), dg.randomString(10, 11));
				writeFile(dout, t);
			}
			dout.write(footer.getBytes());
			dout.flush();
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			fout.close();
		}
	}

	protected static void writeFile(DataOutputStream dout, Test test)
			throws IOException {

		int len = test.getDescription().getBytes().length;
		int rlen = 4 + 8 + 4 + len;
		dout.writeInt(rlen);
		dout.writeInt(test.getId());
		dout.writeDouble(test.getValue());
		dout.writeInt(len);
		dout.write(test.getDescription().getBytes());
	}
}
