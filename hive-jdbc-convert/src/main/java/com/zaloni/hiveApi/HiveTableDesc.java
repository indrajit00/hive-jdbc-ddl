package com.zaloni.hiveApi;

import java.util.List;

public class HiveTableDesc {

	private String databaseName;
	private String tableName;
	private String newTableName;
	private String location;
	private String owner;
	private List<TableFields> column;
	private String rowFormat;
	private String storedAs;
	
	
	//Detailed Table Information fields
	private String createTime;
	private String lastAccessTime;
	private String protectedMOde;
	private String retention;
	private String tableType;
	private String tableParameters;
	
	//Table Storage information fields
	private String serDeLibrary;
	private String inputFormat;
	private String outputFormat;
	private String compressed;
	private String numBuckets;
	private String bucketColumns;
	private String sortColumns;
	private String storageDescParams;
	

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public List<TableFields> getColumn() {
		return column;
	}

	public void setColumn(List<TableFields> column) {
		this.column = column;
	}
	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public String getLastAccessTime() {
		return lastAccessTime;
	}

	public void setLastAccessTime(String lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}

	public String getProtectedMOde() {
		return protectedMOde;
	}

	public void setProtectedMOde(String protectedMOde) {
		this.protectedMOde = protectedMOde;
	}

	public String getRetention() {
		return retention;
	}

	public void setRetention(String retention) {
		this.retention = retention;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public String getTableParameters() {
		return tableParameters;
	}

	public void setTableParameters(String tableParameters) {
		this.tableParameters = tableParameters;
	}

	public String getSerDeLibrary() {
		return serDeLibrary;
	}

	public void setSerDeLibrary(String serDeLibrary) {
		this.serDeLibrary = serDeLibrary;
	}

	public String getInputFormat() {
		return inputFormat;
	}

	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	public String getOutputFormat() {
		return outputFormat;
	}

	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	public String getCompressed() {
		return compressed;
	}

	public void setCompressed(String compressed) {
		this.compressed = compressed;
	}

	public String getNumBuckets() {
		return numBuckets;
	}

	public void setNumBuckets(String numBuckets) {
		this.numBuckets = numBuckets;
	}

	public String getBucketColumns() {
		return bucketColumns;
	}

	public void setBucketColumns(String bucketColumns) {
		this.bucketColumns = bucketColumns;
	}

	public String getSortColumns() {
		return sortColumns;
	}

	public void setSortColumns(String sortColumns) {
		this.sortColumns = sortColumns;
	}

	public String getStorageDescParams() {
		return storageDescParams;
	}

	public void setStorageDescParams(String storageDescParams) {
		this.storageDescParams = storageDescParams;
	}

	public String getRowFormat() {
		return rowFormat;
	}

	public void setRowFormat(String rowFormat) {
		this.rowFormat = rowFormat;
	}

	public String getStoredAs() {
		return storedAs;
	}

	public void setStoredAs(String storedAs) {
		this.storedAs = storedAs;
	}

	public String getNewTableName() {
		return newTableName;
	}

	public void setNewTableName(String newTableName) {
		this.newTableName = newTableName;
	}


}
