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

public class HiveUtility {

	private static final String driver = "org.apache.hive.jdbc.HiveDriver";
	private static Statement state;
	private static Connection connect;

	private HiveUtility() {}// Constructor
	
	
	

	// Method for connection establishment
	public static void checkCon() throws SQLException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			System.out.println("Error : Driver Not Found");
			e.printStackTrace();
			System.exit(1);
		}
		connect = DriverManager.getConnection("jdbc:hive2://192.168.1.135:10000","zaloni","zaloni");
				System.out.println("!!!Connection established successfully!!!");
	}

	/***
	 * Method for Showing Databases
	 **/

	public static ResultSet showDb() throws SQLException {
		System.out.println("!!!! Showing Databases");
		state = connect.createStatement();
		ResultSet show_Db = state.executeQuery("show databases");
		while (show_Db.next()) {
			System.out.println(show_Db.getString(1));
		}
		return show_Db;

	}

	/***
	 * Method for Creating New Database
	 * 
	 * @throws SQLException
	 **/

	public static void createDb(HiveDbDesc db) throws SQLException {

		StringBuffer createHql = new StringBuffer("CREATE DATABASE ")
				.append(db.getDatabaseName());
		if (StringUtils.isNotBlank(db.getDbComments())) {
				createHql.append(" COMMENT '")
				.append(db.getDbComments())
				.append("' ");
		}
		if (StringUtils.isNotBlank(db.getDbLocation())) {
				createHql.append(" LOCATION '")
				.append(db.getDbLocation())
				.append("' ");
		}

		if (MapUtils.isNotEmpty(db.getDbProperties())) {
				createHql.append(" WITH DBPROPERTIES( ");
			for (Entry m : db.getDbProperties().entrySet()) {
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
		System.out.println(db.getDatabaseName() + " is created sucessfully");
	}

	/***
	 * Method for Using a Database
	 * 
	 * @throws SQLException
	 **/
	public static void useDb(HiveDbDesc db) throws SQLException {
		
		state.execute("use " + db.getDatabaseName());
		System.out.println(db.getDatabaseName()+ " is now being used");

	}

	/***
	 * Method to set database properties
	 * 
	 * @throws SQLException
	 **/
	public static void setDbProperty(HiveDbDesc dbPro)throws SQLException {
		
		StringBuilder createHql = new StringBuilder()
			.append("ALTER DATABASE ")
			.append(dbPro.getDatabaseName())
			.append(" SET DBPROPERTIES(");
		for (Entry m : dbPro.getDbProperties().entrySet()) {
			createHql.append(m.getKey())
			.append(" = ")
			.append(m.getValue())
			.append(",");
		}
		createHql.deleteCharAt(createHql.length() - 1);
		createHql.append(")");
		state.execute(createHql.toString());
		System.out.println(createHql);
		System.out.println("Database properties has been updated successfuly");
	}

	/***
	 * Method to set Database Owner
	 * 
	 * @throws SQLException
	 **/
	public static void setDatabaseOwner(HiveDbDesc db)throws SQLException {
		
		StringBuilder createHql = new StringBuilder()
			.append("ALTER DATABASE ")
			.append(db.getDatabaseName())
			.append(" SET OWNER( ")
			.append(" ");
			for (Entry m : db.getDbProperties().entrySet()) {
			createHql.append(m.getKey())
			.append(" = ")
			.append(m.getValue());
		}
		createHql.append(")");
		state.execute(createHql.toString());
		System.out.println(createHql);
		System.out.println("Database owner has been set successfuly");
	}
	
	/***
	 * Method to set Database Location
	 * 
	 * @throws SQLException
	 **/
	public static void setDatabaseLocation(HiveDbDesc db)throws SQLException {
		
		StringBuilder createHql = new StringBuilder()
			.append("ALTER DATABASE ")
			.append(db.getDatabaseName())
			.append(" SET LOCATION( ")
			.append(" ")
			.append(db.getDbLocation())
			.append(")");
		state.execute(createHql.toString());
		System.out.println(createHql);
		System.out.println("Database location has been set successfuly");
	}
	

	
	/***
	 * Method to Create a Table
	 * 
	 * @throws SQLException
	 **/
	public static void createTable(HiveTableDesc tableDesc) throws SQLException {
		try {
			List<TableFields> columnList = tableDesc.getColumn();
			StringBuffer createHql = new StringBuffer("CREATE TABLE ")
				.append(tableDesc.getDatabaseName())
				.append(".")
				.append(tableDesc.getTableName())
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
							createHql.append(columnList.get(i).getColumnType())
									.append(" ")
									.append(" COMMENT ")
									.append(" '")
									.append(columnList.get(i).getComment())
									.append(" ',");
						}
				}
				createHql.deleteCharAt(createHql.length() - 1);
				createHql.append(")");
				System.out.println(createHql);
				state.execute(createHql.toString());
				System.out.println(tableDesc.getTableName()+" * is created Successfully");
				
			} catch (Exception e) {
				System.out.println("Running exception");
				e.printStackTrace();
		}

	}

	/***
	 * Method for alter(Rename a table)
	 * 
	 * @throws SQLException
	 **/
	public static void renameTable(HiveTableDesc tb)throws SQLException {
		StringBuffer createHql= new StringBuffer();
		createHql.append(" ALTER TABLE ")
				.append(tb.getDatabaseName())
				.append(".")
				.append(tb.getTableName())
				.append(" ")
				.append(tb.getNewTableName());
		state.execute(createHql.toString());
		System.out.println("The "+tb.getTableName()+" is renamed with "+tb.getNewTableName());

	}

	/***
	 * Method for alter table properties
	 * 
	 * @throws SQLException
	 **/
	public static void setTableProperty(String tbName,
			Map<String, String> tableProperty) throws SQLException {
		StringBuffer createHql = new StringBuffer("alter table " + tbName
				+ " set tblproperties ('");
		for (Map.Entry m : tableProperty.entrySet()) {
			createHql.append(m.getKey() + " " + m.getValue() + ",");
		}
		createHql.deleteCharAt(createHql.length() - 1);
		createHql.append(")");
		state.execute(createHql.toString());

		System.out.println("Table properties has been updated successfuly");
	}

	/***
	 * Method for serDe properties
	 * 
	 * @throws SQLException
	 **/
	public static void setSerdeProperty(String tbName,Map<String, String> serdeProperty) throws SQLException {
		StringBuffer createHql = new StringBuffer("alter table ")
					.append(tbName)
					.append(" ")
					.append(" set serdeproperties (");
		for (Map.Entry m : serdeProperty.entrySet()) {
			createHql.append("'" + m.getKey() + "'" + "=" + "'" + m.getValue()
					+ "'");
		}
		createHql.deleteCharAt(createHql.length() - 1);
		createHql.append(")");
		state.execute(createHql.toString());

		System.out
				.println("Table Serde properties has been updated successfuly");
	}

	/***
	 * Method for set table storage properties
	 * 
	 * @throws SQLException
	 **/
	public static void setStorageProperty(String tbName,Map<String, String> storageProperty, String numBuckets)throws SQLException {
		StringBuffer createHql = new StringBuffer("alter table " + tbName
				+ " clustered by (");
		for (Map.Entry m : storageProperty.entrySet()) {
			createHql.append(m.getKey() + "," + m.getValue());
		}
		createHql.append("into " + numBuckets + " Buckets");
		state.execute(createHql.toString());

		System.out
				.println("Table Storage properties has been updated successfuly");
	}

	/***
	 * Method for alter table partition(Add)
	 * 
	 * @throws SQLException
	 **/
	public static void addPartition(String tbName,
			Map<String, String> partition, String numBuckets, String location)
			throws SQLException {
		StringBuffer createHql = new StringBuffer("alter table " + tbName
				+ " add partition (");
		for (Map.Entry m : partition.entrySet()) {
			createHql.append(m.getKey() + "=" + "'" + m.getValue() + "'");
		}
		createHql.append(") location" + "'" + location + "'");
		state.execute(createHql.toString());

		System.out
				.println("Table Storage properties has been updated successfuly");
	}

	/***
	 * Method for alter table partition(Rename)
	 * 
	 * @throws SQLException
	 **/
	public static void renamePartition(String tbName,
			Map<String, String> oldNamePartition,
			Map<String, String> newNamePartition) throws SQLException {
		StringBuffer createHql = new StringBuffer("alter table " + tbName
				+ " partition ");
		for (Map.Entry m : oldNamePartition.entrySet()) {
			createHql.append(m.getKey() + "=" + "'" + m.getValue() + "'");
		}
		createHql.append(" rename to partition ");
		for (Map.Entry m : newNamePartition.entrySet()) {
			createHql.append(m.getKey() + "=" + "'" + m.getValue() + "'");
		}
		state.execute(createHql.toString());

		System.out
				.println("Table Storage properties has been updated successfuly");
	}

	/***
	 * Method for alter table partition(exchange partition)
	 * 
	 * @throws SQLException
	 **/
	public static void exchangePartition(String table1, String table2,
			Map<String, String> partitionSpec) throws SQLException {
		StringBuffer createHql = new StringBuffer("alter table " + table2
				+ " exchange partition (");
		for (Map.Entry m : partitionSpec.entrySet()) {
			createHql.append(m.getKey() + "=" + "'" + m.getValue() + "'");
		}
		createHql.append(") with table " + table1);
		state.execute(createHql.toString());

		System.out.println("Table Storage properties has been updated successfuly");
	}
	
	
		

	/***
	 * Method for Alter Column
	 * 
	 * @throws SQLException
	 **/
	public static void alterColumn(HiveTableDesc tb) throws SQLException {
		List<TableFields> tableField=tb.getColumn();
		StringBuffer createHql= new StringBuffer(" ALTER TABLE ");
		for(int i=0;i<=tableField.size();i++) {
			createHql.append(tb.getTableName())
					.append(" CHANGE COLUMN ")
					.append(tableField.get(i).getColumnName())
					.append(" ")
					.append(tableField.get(i).getColumnNewName());
					if(StringUtils.isNotBlank(tableField.get(i).getColumnType().getPrecision())&&StringUtils.isNotBlank(tableField.get(i).getColumnType().getScale()))
					{
						createHql.append(tableField.get(i).getColumnType())
						.append("(")
						.append(tableField.get(i).getColumnType().getPrecision())
						.append(",")
						.append(tableField.get(i).getColumnType().getScale())
						.append(")");
					}
					else
					{
						createHql.append(tableField.get(i).getColumnType())
						.append(" COMMENT ")
						.append(" '")
						.append(tableField.get(i).getComment())
						.append("'");
					}
					createHql.append(")");
					System.out.println(createHql);
					state.execute(createHql.toString());
		}
		
	}
	
	/***
	 * Method for Replace Column
	 * 
	 * @throws SQLException
	 **/
	public static void replaceColumn(HiveTableDesc tb) throws SQLException
	{
		List<TableFields> tableField=tb.getColumn();
		StringBuffer createHql= new StringBuffer(" ALTER TABLE ");
		for(int i=0;i<=tableField.size();i++) {
			createHql.append(tb.getTableName())
					.append(" REPLACE COLUMNS (")
					.append(tableField.get(i).getColumnName())
					.append(" ");
					if(StringUtils.isNotBlank(tableField.get(i).getColumnType().getPrecision())&&StringUtils.isNotBlank(tableField.get(i).getColumnType().getScale()))
					{
						createHql.append(tableField.get(i).getColumnType())
						.append("(")
						.append(tableField.get(i).getColumnType().getPrecision())
						.append(",")
						.append(tableField.get(i).getColumnType().getScale())
						.append(")");
					}
					else
					{
						createHql.append(tableField.get(i).getColumnType())
						.append(" COMMENT ")
						.append(" '")
						.append(tableField.get(i).getComment())
						.append(",");
					}
					createHql.deleteCharAt(createHql.length() - 1);
					createHql.append(")");
					System.out.println(createHql);
					state.execute(createHql.toString());
		}
	}
		
}
