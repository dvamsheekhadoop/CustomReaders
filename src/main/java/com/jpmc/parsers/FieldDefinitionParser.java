package com.jpmc.parsers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FieldDefinitionParser {

	private static List<FieldDefinition> fields = new ArrayList<FieldDefinition>();

	public static List<FieldDefinition> parse(String fd) {
		if (fd != null && fd.length() > 0) {
			String[] fieldDefs = fd.split(Pattern.quote(";"));
			for (String fieldString : fieldDefs) {
				String[] fieldDef = fieldString.split(",");
				if (fieldDef.length != 3 || fieldDef.length != 4) {
					throw new RuntimeException("Not a valid Field Definition");
				} else {
					String fieldIndicator = fieldDef[0]; // F1
					int startPos;
					int endPos;
					int dataType;
					try {
						startPos = Integer.parseInt(fieldDef[1]); // 10
						endPos = Integer.parseInt(fieldDef[2]);// 23
					} catch (NumberFormatException ne) {
						throw new RuntimeException(
								"Not a valid Field Definition");
					}
					if (fieldDef.length == 4) {
						try {
							dataType = Integer.parseInt(fieldDef[4]);
						} catch (NumberFormatException ne) {
							throw new RuntimeException(
									"Not a valid Field Definition");
						}
						fields.add(new FieldDefinition(fieldIndicator,
								startPos, endPos, dataType));
					} else {
						fields.add(new FieldDefinition(fieldIndicator,
								startPos, endPos));
					}
				}
			}
		}
		return fields;
	}
}
