package com.zaloni.hiveApi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

/**
 * @author kzaman This is a Utility Class for creating Database,Table
 */
public class HiveCreate {

	private HiveCreate() {
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
	 *            type,column comment
	 * @throws SQLException
	 */
	public static boolean createTable(HiveTableDesc tableDesc) throws SQLException {

		try {
			List<Column> columnList = tableDesc.getColumn();
			StringBuffer createHql = new StringBuffer("CREATE ");
			
			if(tableDesc.isTemporary()==true)
			{
				createHql.append(" TEMPORARY ");
			}
			if (StringUtils.isNotBlank(tableDesc.getTableType())) {
				createHql.append(tableDesc.getTableType());
			}
			createHql.append(" TABLE ")
					.append(" ")
					.append(tableDesc.getTableName()).append("( ");

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
	public static boolean closeConnection() throws SQLException
	{
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
		ResultSet show_Tab = HiveQueryExecutor.executeQuery("show tables in "+databaseName);
		while (show_Tab.next()) {
			System.out.println(show_Tab.getString(1));
		}

	}

}
