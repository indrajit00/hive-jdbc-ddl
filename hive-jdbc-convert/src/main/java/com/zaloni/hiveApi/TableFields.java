package com.zaloni.hiveApi;

public class TableFields {
	private String columnName;
	private HiveDataType columnType;
	private String comment;
	

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public HiveDataType getColumnType() {
		return columnType;
	}

	public void setColumnType(HiveDataType columnType) {
		this.columnType = columnType;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

}
