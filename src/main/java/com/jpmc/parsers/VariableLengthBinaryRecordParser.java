package com.jpmc.parsers;

import java.nio.ByteBuffer;

public class VariableLengthBinaryRecordParser {

	public static BinaryObj parser(byte[] binaryBytes) {
		ByteBuffer bb = ByteBuffer.wrap(binaryBytes);
		int id = bb.getInt();
		double value = bb.getDouble();
		int strLength = bb.getInt();
		byte[] stringBytes = new byte[strLength];
		bb.get(stringBytes);
		String desc = new String(stringBytes);

		return new BinaryObj(id, value, desc);
	}
}
