package com.zaloni.hiveApi;

public class DataType {

	private HiveDataType dataType;
	private String precision;
	private String scale;
	
	public HiveDataType getDataType() {
		return dataType;
	}
	public void setDataType(HiveDataType dataType) {
		this.dataType = dataType;
	}
	public String getPrecision() {
		return precision;
	}
	public void setPrecision(String precision) {
		this.precision = precision;
	}
	public String getScale() {
		return scale;
	}
	public void setScale(String scale) {
		this.scale = scale;
	}
	
}

