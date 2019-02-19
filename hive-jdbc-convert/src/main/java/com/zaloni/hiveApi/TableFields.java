package com.zaloni.hiveApi;

public class TableFields {
	private String columnName;
	private String columnNewName;
	private DataType columnType;
	private String comment;
	
	

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public DataType getColumnType() {
		return columnType;
	}

	public void setColumnType(DataType columnType) {
		this.columnType = columnType;
		
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getColumnNewName() {
		return columnNewName;
	}

	public void setColumnNewName(String columnNewName) {
		this.columnNewName = columnNewName;
	}

	
	

}
