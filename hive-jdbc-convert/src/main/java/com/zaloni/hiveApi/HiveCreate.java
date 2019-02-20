package com.zaloni.hiveApi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import com.zaloni.hiveApi.HiveTableDesc;

/**
 * @author kzaman
 * This is a Utility Class for creating Database,Table
 */
public class HiveCreate {

	private static final String driver = "org.apache.hive.jdbc.HiveDriver";
	private static Statement state;
	private static Connection connect;

	private HiveCreate() {}// Constructor
	
	
	
	/**
	 * @throws SQLException
	 * This method is used to establish connection with Hive
	 */
	public static void establishConnection() throws SQLException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			System.out.println("Error : Driver Not Found");
			e.printStackTrace();
			System.exit(1);
		}
		connect = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
				System.out.println("!!!Connection established successfully!!!");
	}

	/***
	 * Method for Showing Databases
	 **/

	/**
	 * @throws SQLException
	 * This method shows all databases
	 */
	public static void showDb() throws SQLException {
		System.out.println("!!!! Showing Databases");
		state = connect.createStatement();
		ResultSet show_Db = state.executeQuery("show databases");
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
	 * @param databaseDesc object containing database name,database comment,database location and database properties
	 * @throws SQLException
	 */
	public static void createDatabase(HiveDbDesc databaseDesc) throws SQLException {

		StringBuffer createHql = new StringBuffer("CREATE DATABASE ")
							.append(databaseDesc.getDatabaseName());
		if (StringUtils.isNotBlank(databaseDesc.getDbComments())) {
					createHql.append(" COMMENT '")
							.append(databaseDesc.getDbComments())
							.append("' ");
		}
		if (StringUtils.isNotBlank(databaseDesc.getDbLocation())) {
					createHql.append(" LOCATION '")
							.append(databaseDesc.getDbLocation())
							.append("' ");
		}

		if (MapUtils.isNotEmpty(databaseDesc.getDbProperties())) {
					createHql.append(" WITH DBPROPERTIES( ");
			for (Entry m : databaseDesc.getDbProperties().entrySet()) {
					createHql.append(" '")
							.append(m.getKey())
							.append("' = '")
							.append(m.getValue())
							.append("',");
			}
					createHql.deleteCharAt(createHql.length() - 1);
					createHql.append(")");
		}
		
		System.out.println("Final statement is - " + createHql.toString());
		state.execute(createHql.toString());
		System.out.println(databaseDesc.getDatabaseName() + " is created sucessfully");
	}

	
	
	
	
	
	

	
	/***
	 * Method to Create a Table
	 * 
	 * @throws SQLException
	 **/
	/**
	 * @param tableDesc  object containing table name,table type,column name,column type,column comment
	 * @throws SQLException
	 */
	public static void createTable(HiveTableDesc tableDesc) throws SQLException {
		try {
			List<Column> columnList = tableDesc.getColumn();
			StringBuffer createHql = new StringBuffer("CREATE TABLE ")
								.append(tableDesc.getDatabaseName())
								.append(".");
			if(tableDesc.getExternalTable()==true)
			{
						createHql.append(" EXTERNAL ");
			}
						createHql.append(tableDesc.getTableName())
								.append("( ");

			for (int i = 0; i < columnList.size(); i++) {
				createHql.append(columnList.get(i).getColumnName())
						.append(" ");
						if (StringUtils.isNotBlank(columnList.get(i).getColumnType().getPrecision())&&StringUtils.isNotBlank(columnList.get(i).getColumnType().getScale())) 
						{
							createHql.append(columnList.get(i).getColumnType().getDataType())
									.append("(")
									.append(columnList.get(i).getColumnType().getPrecision())
									.append(",")
									.append(columnList.get(i).getColumnType().getScale())
									.append(")");
									
						}
						else 
						{
							createHql.append(columnList.get(i).getColumnType());
						}
							createHql.append(" ")
									.append(" COMMENT ")
									.append("'")
									.append(columnList.get(i).getComment())
									.append("'")
									.append(",");
				}
							createHql.deleteCharAt(createHql.length() - 1);
							createHql.append(")");
				if(StringUtils.isNotBlank(tableDesc.getRowFormat()))
						{
							createHql.append(" ROW FORMAT ")
									.append(tableDesc.getRowFormat())
									.append(" ");
						}
				if(StringUtils.isNotBlank(tableDesc.getFieldsTerminatedBy()))
				{
					createHql.append(" FIELDS TERMINATED BY ")
							.append("'")
							.append(tableDesc.getFieldsTerminatedBy())
							.append("' ");
				}
				if(StringUtils.isNotBlank(tableDesc.getStoredAs()))
				{
							createHql.append(" ")
									.append(tableDesc.getStoredAs())
									.append(" ");
				}
				if(StringUtils.isNotBlank(tableDesc.getLocation()))
				{
							createHql.append(" '")
									.append(tableDesc.getLocation())
									.append("' ");
				}
				System.out.println(createHql);
				state.execute(createHql.toString());
				System.out.println(tableDesc.getTableName()+" * is created Successfully");
				
			} catch (Exception e) {
				System.out.println("Running exception");
				e.printStackTrace();
		}

	}
		
}
