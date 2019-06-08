package com.zaloni.hiveApi;

import java.io.FileWriter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.IOException;
import java.sql.ResultSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.sql.ResultSetMetaData;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.abort_txn;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.zaloni.hiveApi.HiveTableDesc;

/**
 * @author rmour This is a Utility Class for creating Database,Table
 */
public class HiveTableOperations {

	private static final String Entry = null;

	private HiveTableOperations() {
	}// Constructor

	/***
	 * Method for Showing Databases
	 **/

	/**
	 * @throws SQLException
	 *             This method shows all databases
	 */
	public static void showDb() throws SQLException {
		System.out.println("!!!! Showing Databases");
		ResultSet show_Db = HiveQueryExecutor.executeQuery("show databases");
		while (show_Db.next()) {
			System.out.println(show_Db.getString(1));
		}

	}

	/***
	 * Method for Creating New Database
	 * 
	 * @throws SQLException
	 **/

	/**
	 * @param databaseDesc
	 *            object containing database name,database comment,database location
	 *            and database properties
	 * @throws SQLException
	 */
	public static boolean createDatabase(HiveDbDesc databaseDesc) throws SQLException {

		StringBuffer createHql = new StringBuffer("CREATE DATABASE ").append(databaseDesc.getDatabaseName());
		if (StringUtils.isNotBlank(databaseDesc.getDbComments())) {
			createHql.append(" COMMENT '").append(databaseDesc.getDbComments()).append("' ");
		}
		if (StringUtils.isNotBlank(databaseDesc.getDbLocation())) {
			createHql.append(" LOCATION '").append(databaseDesc.getDbLocation()).append("' ");
		}

		if (MapUtils.isNotEmpty(databaseDesc.getDbProperties())) {
			createHql.append(" WITH DBPROPERTIES( ");
			for (Entry m : databaseDesc.getDbProperties().entrySet()) {
				createHql.append(" '").append(m.getKey()).append("' = '").append(m.getValue()).append("',");
			}
			createHql.deleteCharAt(createHql.length() - 1);
			createHql.append(")");
		}

		System.out.println("Final statement is - " + createHql.toString());
		HiveConnectionProvider.getStatement().execute(createHql.toString());
		System.out.println(databaseDesc.getDatabaseName() + " is created sucessfully");
		return true;
	}

	/***
	 * Method to Create a Table
	 * 
	 * @throws SQLException
	 **/
	/**
	 * @param tableDesc
	 *            object containing table name,table type,column name,column
	 *            type,column comment,Row format for delimited and serde class name
	 *            stored as,location
	 * @throws SQLException
	 */
	

	public static boolean createTable(HiveTableDesc tableDesc) throws SQLException {

		try {
			List<Column> columnList = tableDesc.getColumn();
			StringBuffer createHql = new StringBuffer("CREATE ");

			if (tableDesc.isTemporary() == true) {
				createHql.append(" TEMPORARY ");
			}
				
			if (StringUtils.isNotBlank(tableDesc.getTableType())) {
				createHql.append(tableDesc.getTableType());
			}
			createHql.append(" TABLE ").append(" ").append(tableDesc.getTableName()).append("( ");

			for (int i = 0; i < columnList.size(); i++) {
				createHql.append(columnList.get(i).getColumnName()).append(" ");
				if (StringUtils.isNotBlank(columnList.get(i).getColumnType().getPrecision())
						&& StringUtils.isNotBlank(columnList.get(i).getColumnType().getScale())) {
					createHql.append(columnList.get(i).getColumnType().getDataType()).append("(")
							.append(columnList.get(i).getColumnType().getPrecision()).append(",")
							.append(columnList.get(i).getColumnType().getScale()).append(")");

				} else {
					createHql.append(columnList.get(i).getColumnType().getDataType());
				}
				createHql.append(" ").append(" COMMENT ").append("'").append(columnList.get(i).getComment()).append("'")
						.append(",");
			}
			createHql.deleteCharAt(createHql.length() - 1);
			createHql.append(")");

			if (StringUtils.isNotBlank(tableDesc.getRowFormat())) {
				createHql.append(" ROW FORMAT ").append("").append(tableDesc.getRowFormat());

			}

			if (StringUtils.isNotBlank(tableDesc.getRowFormatSerdeName())) {
				createHql.append(" ROW FORMAT SERDE ");

				createHql.append("'").append(tableDesc.getRowFormatSerdeName()).append("'");

			}

			if (StringUtils.isNotBlank(tableDesc.getStoredAs())) {
				createHql.append(" ").append(tableDesc.getStoredAs()).append(" ");
			}
			if (StringUtils.isNotBlank(tableDesc.getLocation())) {
				createHql.append(" '").append(tableDesc.getLocation()).append("' ");
			}
			System.out.println(createHql);
			HiveConnectionProvider.getStatement().execute(createHql.toString());

			System.out.println(tableDesc.getTableName() + " * is created Successfully");

		} catch (Exception e) {
			System.out.println("Running exception");
			e.printStackTrace();
		}
		return true;

	}

	/**
	 * Method for closing connection
	 * 
	 * 
	 * @throws SQLException
	 */
	public static boolean closeConnection() throws SQLException {
		HiveConnectionProvider.closeConnection();
		System.out.println("Connection closed successfully");
		return true;
	}

	/**
	 * Method that shows tables in the default database
	 * 
	 * 
	 * @throws SQLException
	 */
	public static void showTable(String databaseName) throws SQLException {
		System.out.println("!!!! Showing Tables");
		ResultSet show_Tab = HiveQueryExecutor.executeQuery("show tables in " + databaseName);
		while (show_Tab.next()) {
			System.out.println(show_Tab.getString(1));
		}

	}
	
	
	public static HiveTableDesc fetchTableDetails(String databaseName, String tableName) throws SQLException, IOException {

	
	ResultSet tableResultSet = HiveQueryExecutor
			.executeQuery(String.format("desc formatted %s.%s", databaseName, tableName));
	HiveTableDesc getTab = new HiveTableDesc();
	while (tableResultSet.next()) {
//		System.out.println( tableResultSet.getString(1) + " || " + tableResultSet.getString(2) + " || "
//			+ tableResultSet.getString(3));
		
		getTab.setTableName(tableName);
		
		List<Column> columnList=new ArrayList<Column>();
			 if (tableResultSet.getString(1).startsWith("# col_name")) {
	             while (tableResultSet.next()) {
	                if (StringUtils.isNotBlank(tableResultSet.getString(1)) && tableResultSet.getString(1).startsWith("#")) {
	                       break;
	                  }
	        
	                  if (StringUtils.isNotBlank(tableResultSet.getString(1)) && StringUtils.isNotBlank(tableResultSet.getString(2))) {
  
	                    	   Column column= new Column();
	                    	   column.setColumnName(tableResultSet.getString(1).trim());
	                    	   DataType dataType=new DataType();
	                    	   dataType.setDataType(tableResultSet.getString(2).trim());
	                    	   column.setColumnType(dataType);
	                    	   column.setComment(tableResultSet.getString(3).trim());
	                    	   
	                    	   columnList.add(column);
	                     
	                  }
	                  getTab.setColumn(columnList);
	             }

	        }

		if (tableResultSet.getString(1).contains("Database")) {
			getTab.setDatabaseName(tableResultSet.getString(2));
		}
		if (tableResultSet.getString(1).contains("Owner")) {
			getTab.setOwner(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("CreateTime")) {
			getTab.setCreateTime(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("LastAccessTime")) {
			getTab.setLastAccessTime(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Protect Mode")) {
			getTab.setProtectedMOde(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Retention")) {
			getTab.setRetention(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Location")) {
			getTab.setLocation(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Table Type")) {
			getTab.setTableType(tableResultSet.getString(2));
		}
		if (tableResultSet.getString(1).contains("Table Parameters")) {	
//			tableResultSet.next();
			Map<String,String> tableParameterMap =  new LinkedHashMap<String,String>();
			while (tableResultSet.next()) {
					tableParameterMap.put(tableResultSet.getString(2), tableResultSet.getString(3));
				if (StringUtils.isBlank(tableResultSet.getString(2)))
					break;
			}
			getTab.setTableParameters(tableParameterMap);
		}
		if(tableResultSet.getString(1).contains("SerDe Library")) {
			getTab.setSerDeLibrary(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("InputFormat")) {
			getTab.setInputFormat(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("OutputFormat")) {
			getTab.setOutputFormat(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Compressed")) {
			getTab.setCompressed(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Num Buckets")) {
			getTab.setNumBuckets(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Bucket Columns")) {
			getTab.setBucketColumns(tableResultSet.getString(2));
		}
		if(tableResultSet.getString(1).contains("Sort Columns")) {
			getTab.setSortColumns(tableResultSet.getString(2));
		}
		if (tableResultSet.getString(1).contains("Storage Desc Params")) {
			Map<String, String> storageDescParam = new HashMap<String, String>();
			while (tableResultSet.next()) {
					storageDescParam.put(tableResultSet.getString(2), tableResultSet.getString(3));
					
				if (tableResultSet.getString(2).isEmpty())
					break;
			}
			getTab.setStorageDescParams(storageDescParam);
		}
	}

//		
	System.out.println("========================================================================================");

	System.out.println("TableName: " + getTab.getTableName());
	
	List<Column> columnList=getTab.getColumn();
	System.out.println("Columns: ");
	for(int i=0;i<columnList.size();i++)
	{
		System.out.println(columnList.get(i).getColumnName()+" || "+columnList.get(i).getColumnType().getDataType()+" || "+columnList.get(i).getComment());
		
	}
	
	
	System.out.println("DB: " + getTab.getDatabaseName());
	System.out.println(" OWNER:" + getTab.getOwner());
	System.out.println("Create Time: " + getTab.getCreateTime());
	System.out.println("Last Access Time: "+getTab.getLastAccessTime());
	System.out.println("Protect Mode: " +getTab.getProtectedMOde());
	System.out.println("Retention: "+getTab.getRetention());
	System.out.println("Location: "+getTab.getLocation());
	System.out.println("Table Type: "+getTab.getTableType());
	System.out.println("Table Parameters: " +getTab.getTableParameters());
	System.out.println("Serde Library: "+ getTab.getSerDeLibrary());
	System.out.println("Input Format: "+ getTab.getInputFormat());
	System.out.println("Output Format: "+ getTab.getOutputFormat());
	System.out.println("Compressed: "+ getTab.getCompressed());
	System.out.println("Num Buckets: "+ getTab.getNumBuckets());
	System.out.println("Bucket Columns: "+ getTab.getBucketColumns());
	System.out.println("Sort Columns: "+ getTab.getSortColumns());
	System.out.println("Storage Desc Params: "+ getTab.getStorageDescParams());
	
	return  getTab;
	}

}
