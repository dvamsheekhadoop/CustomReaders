package com.jpmc.parsers;

public class FieldDefinition {

	private String fieldName;
	private int startPos;
	private int endPos;
	private int dataType;

	public FieldDefinition(String fname, int start, int end) {

		this.fieldName = fname;
		this.startPos = start;
		this.endPos = end;
	}

	public FieldDefinition(String fname, int start, int end, int dataType) {

		this.fieldName = fname;
		this.startPos = start;
		this.endPos = end;
		this.dataType = dataType;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public int getStartPos() {
		return startPos;
	}

	public void setStartPos(int startPos) {
		this.startPos = startPos;
	}

	public int getEndPos() {
		return endPos;
	}

	public void setEndPos(int endPos) {
		this.endPos = endPos;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

}
