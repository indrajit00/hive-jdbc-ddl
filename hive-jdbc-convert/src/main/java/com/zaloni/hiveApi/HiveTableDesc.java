package com.zaloni.hiveApi;

import java.util.ArrayList;


import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


public class HiveTableDesc {

	/**
	 * rowFormat is for data Delimited if called
	 * rowFormatSerdeName is for serde class to called
	 */
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	private String databaseName;
	private String tableName;
	private String newTableName;
	private boolean deleted;
	private boolean fetchTable;
	private String location;
	private String owner;
	private List<Column> column;
	private String rowFormat;
	private String storedAs;
	private String rowFormatSerdeName;
	private Map<String,String> serdeProperties;
	private List<Column> partitionColumn;
	private String tableType;
	private boolean ifPartition;
	private boolean temporary;
	
	
	//Detailed Table Information fields
	private String createTime;
	private String lastAccessTime;
	private String protectedMOde;
	private String retention;
	private Map<String,String> tableParameters;
	
	private String dropType;
	
	//Table Storage information fields
	private String serDeLibrary;
	private String inputFormat;
	private String outputFormat;
	private String compressed;
	private String numBuckets;
	private String bucketColumns;
	private String sortColumns;
	private Map<String,String> storageDescParams;
	
	
	public Map<String, String> getStorageDescParams() {
		return storageDescParams;
	}

	public void setStorageDescParams(Map<String, String> storageDescParams) {
		this.storageDescParams = storageDescParams;
	}

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

	public List<Column> getColumn() {
		return column;
	}

	public void setColumn(List<Column> columnList) {
		this.column = columnList;
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

	public Map<String,String> getTableParameters() {
		return tableParameters;
	}
	public void setTableParameters(Map<String,String> tableParameters) {
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

	public String getRowFormatSerdeName() {
		return rowFormatSerdeName;
	}

	public void setRowFormatSerdeName(String rowFormatSerdeName) {
		this.rowFormatSerdeName = rowFormatSerdeName;
		
	}

	public String getDropType() {
		return dropType;
	}

	public void setDropType(String dropType) {
		this.dropType = dropType;
	}

	public List<Column> getPartitionColumn() {
		return partitionColumn;
	}

	public void setPartitionColumn(List<Column> partitionColumn) {
		this.partitionColumn = partitionColumn;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public Boolean getIfPartition() {
		return ifPartition;
	}

	public void setIfPartition(Boolean ifPartition) {
		this.ifPartition = ifPartition;
	}

	public Map<String, String> getSerdeProperties() {
		return serdeProperties;
	}

	public void setSerdeProperties(Map<String, String> serdeProperties) {
		this.serdeProperties = serdeProperties;
	}

	public boolean isTemporary() {
		return temporary;
	}

	public void setTemporary(boolean temporary) {
		this.temporary = temporary;
	}
	
	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
	
	public boolean isFetchTable() {
		return fetchTable;
	}

	public void setFetchTable(boolean fetchTable) {
		this.fetchTable = fetchTable;
	}


	@Override
	public String toString() {
		return "HiveTableDesc [databaseName=" + databaseName + ", tableName=" + tableName + ", newTableName="
				+ newTableName + ", deleted=" + deleted + ", fetchTable=" + fetchTable + ", location=" + location
				+ ", owner=" + owner + ", column=" + column + ", rowFormat=" + rowFormat + ", storedAs=" + storedAs
				+ ", rowFormatSerdeName=" + rowFormatSerdeName + ", serdeProperties=" + serdeProperties
				+ ", partitionColumn=" + partitionColumn + ", tableType=" + tableType + ", ifPartition=" + ifPartition
				+ ", temporary=" + temporary + ", createTime=" + createTime + ", lastAccessTime=" + lastAccessTime
				+ ", protectedMOde=" + protectedMOde + ", retention=" + retention + ", tableParameters="
				+ tableParameters + ", dropType=" + dropType + ", serDeLibrary=" + serDeLibrary + ", inputFormat="
				+ inputFormat + ", outputFormat=" + outputFormat + ", compressed=" + compressed + ", numBuckets="
				+ numBuckets + ", bucketColumns=" + bucketColumns + ", sortColumns=" + sortColumns
				+ ", storageDescParams=" + storageDescParams + "]";
	}
	
	


}