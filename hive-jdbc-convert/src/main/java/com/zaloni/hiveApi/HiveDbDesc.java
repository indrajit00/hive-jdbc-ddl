package com.zaloni.hiveApi;

import java.util.Map;

public class HiveDbDesc {
	private String databaseName;
	private String dbComments;
	private Map<String,String> dbProperties;
	private boolean ifNotExists;
	private String dbLocation;
	private Map<String,String> owner;
	
	 
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getDbComments() {
		return dbComments;
	}
	public void setDbComments(String dbComments) {
		this.dbComments = dbComments;
	}
	public Map<String, String> getDbProperties() {
		return dbProperties;
	}
	public void setDbProperties(Map<String, String> dbProperties) {
		this.dbProperties = dbProperties;
	}
	public boolean isIfNotExists() {
		return ifNotExists;
	}
	public void setIfNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
	}
	public String getDbLocation() {
		return dbLocation;
	}
	public void setDbLocation(String dbLocation) {
		
		this.dbLocation = dbLocation;
	}
	public Map<String, String> getOwner() {
		return owner;
	}
	public void setOwner(Map<String, String> owner) {
		this.owner = owner;
	}

}
