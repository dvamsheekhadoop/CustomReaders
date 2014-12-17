package com.jpmc.parsers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class RecordParser {

	public static class DataType {
		public static final int BYTE = 0;
		public static final int SHORT = 1;
		public static final int INT = 2;
		public static final int LONG = 3;
		public static final int FLOAT = 4;
		public static final int DOUBLE = 5;
		public static final int STRING = 6;
		public static final int BOOLEAN = 7;

	}

	public static BinaryObj parseVariableLengthBinaryRecord(byte[] binaryBytes) {
		ByteBuffer bb = ByteBuffer.wrap(binaryBytes);
		int id = bb.getInt();
		double value = bb.getDouble();
		int strLength = bb.getInt();
		byte[] stringBytes = new byte[strLength];
		bb.get(stringBytes);
		String desc = new String(stringBytes);

		return new BinaryObj(id, value, desc);
	}

	public static List<String> parseDelimetedRecord(String record,
			String delimeter) {

		List<String> fields = new ArrayList<String>();
		if (record != null && record.length() > 0) {
			fields = Arrays.asList(record.split(Pattern.quote(delimeter)));
		}
		return fields;
	}

	public static List<String> parseFixedWidthTextRecord(String record,
			String fieldDefinition) {
		List<FieldDefinition> fds = FieldDefinitionParser
				.parse(fieldDefinition);

		List<String> fields = new ArrayList<String>();
		for (FieldDefinition fd : fds) {
			fields.add(record.substring(fd.getStartPos(), fd.getEndPos() + 1));
		}
		return fields;
	}

	public static List<?> parseFixedWidthBinaryRecord(byte[] record,
			String fieldDefinition) {
		List<FieldDefinition> fds = FieldDefinitionParser
				.parse(fieldDefinition);
		ByteBuffer bb = ByteBuffer.wrap(record);
		List fields = new ArrayList();
		for (FieldDefinition fd : fds) {
			switch (fd.getDataType()) {
			case DataType.SHORT:
				fields.add(bb.getShort());
				break;
			case DataType.BYTE:
				fields.add(bb.get());
				break;
			case DataType.INT:
				fields.add(bb.getInt());
				break;
			case DataType.LONG:
				fields.add(bb.getLong());
				break;
			case DataType.FLOAT:
				fields.add(bb.getFloat());
				break;
			case DataType.DOUBLE:
				fields.add(bb.getDouble());
				break;
			case DataType.STRING:
				byte[] stringBytes = new byte[fd.getEndPos() - fd.getStartPos()];
				fields.add(bb.get(stringBytes));
				break;
			default:
				break;
			}
		}
		return fields;
	}
}
