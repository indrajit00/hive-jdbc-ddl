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
	 * Method for Dropping a Database
	 * 
	 * @throws SQLException
	 **/
	public static void dropDatabase(String databaseName) throws SQLException
	{
		StringBuffer createHql= new StringBuffer("DROP DATABASE ")
							.append(databaseName);
		
		state.execute(createHql.toString());
		System.out.println(databaseName+" is dropped successfully");
	}
	
	public static void dropDatabase(String databaseName,String dropType) throws SQLException
	{
		StringBuffer createHql= new StringBuffer("DROP DATABASE ")
							.append(databaseName)
							.append(" ")
							.append(dropType);
		
		state.execute(createHql.toString());
		System.out.println(databaseName+" is dropped successfully");
	}
	
	/***
	 * Method for Using a Database
	 * 
	 * @throws SQLException
	 **/
	public static void useDb(String databaseName) throws SQLException {
		
		state.execute("use " + databaseName);
		System.out.println(databaseName+ " is now being used");

	}

	/***
	 * Method to update database properties
	 * 
	 * @throws SQLException
	 **/
	public static void updateDatabaseProperties(String databaseName,Map<String,String> databaseProperties)throws SQLException {
		
		StringBuilder createHql = new StringBuilder()
							.append("ALTER DATABASE ")
							.append(databaseName)
							.append(" SET DBPROPERTIES(");
		for (Entry m : databaseProperties.entrySet()) {
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
	 * Method to update Database Owner
	 * 
	 * @throws SQLException
	 **/
	public static void updateDatabaseOwner(String databaseName,String role)throws SQLException {
		
		StringBuilder createHql = new StringBuilder()
							.append("ALTER DATABASE ")
							.append(databaseName)
							.append(" SET OWNER( ")
							.append(" ")
							.append(role);
							
		state.execute(createHql.toString());
		System.out.println(createHql);
		System.out.println("Database owner has been set successfuly");
	}
	
	/***
	 * Method to update Database Location
	 * 
	 * @throws SQLException
	 **/
	public static void updateDatabaseLocation(String databaseName,String location)throws SQLException {
		
		StringBuilder createHql = new StringBuilder()
							.append("ALTER DATABASE ")
							.append(databaseName)
							.append(" SET LOCATION( ")
							.append(" ")
							.append(location);
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

	/***
	 * Method for Dropping a Table
	 * 
	 * @throws SQLException
	 **/
	public static void dropTable(String tableName) throws SQLException
	{
		StringBuffer createHql= new StringBuffer("DROP TABLE ")
							.append(tableName);
		state.execute(createHql.toString());
		System.out.println(tableName+" is dropped successfully");
	}
	
	public static void dropTable(String tableName,String dropType) throws SQLException
	{
		StringBuffer createHql= new StringBuffer("DROP TABLE ")
							.append(tableName)
							.append(" ")
							.append(dropType);
		state.execute(createHql.toString());
		System.out.println(tableName+" is dropped successfully");
	}
	
	
	/***
	 * Method for update(Rename a table)
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
	 * Method for update table properties
	 * 
	 * @throws SQLException
	 **/
	public static void updateTableProperties(String tableName,Map<String, String> tableProperties) throws SQLException {
		StringBuffer createHql = new StringBuffer("ALTER TABLE ")
							.append(tableName)
							.append(" SET TBLPROPERTIES('");
		for (Map.Entry m : tableProperties.entrySet()) {
					createHql.append(m.getKey() + " " + m.getValue() + ",");
		}
					createHql.deleteCharAt(createHql.length() - 1)
							.append(")");
		state.execute(createHql.toString());

		System.out.println("Table properties has been updated successfuly");
	}

	/***
	 * Method for update serDe properties
	 * 
	 * @throws SQLException
	 **/
	public static void updateSerdeProperties(String tableName,Map<String, String> serdeProperties) throws SQLException {
		StringBuffer createHql = new StringBuffer("ALTER TABLE ")
							.append(tableName)
							.append(" ")
							.append(" SET SERDEPROPERTIES (");
		for (Map.Entry m : serdeProperties.entrySet()) 
		{
					createHql.append("'")
							.append(m.getKey())
							.append("'")
							.append("=")
							.append("'")
							.append(m.getValue())
							.append("'");
		}
					createHql.deleteCharAt(createHql.length() - 1)
							.append(")");
		state.execute(createHql.toString());

		System.out
				.println("Table Serde properties has been updated successfuly");
	}

	/***
	 * Method for update table storage properties
	 * 
	 * @throws SQLException
	 **/
	public static void updateStorageProperties(String tableName,Map<String, String> storageProperties, String numOfBuckets)throws SQLException {
		StringBuffer createHql = new StringBuffer(" ALTER TABLE ")
							.append(tableName)
							.append(" CLUSTERED BY (");
		for (Map.Entry m : storageProperties.entrySet()) 
		{
					createHql.append(m.getKey())
							.append(",")
							.append(m.getValue());
		}
					createHql.append("INTO ")
							.append(numOfBuckets)
							.append("BUCKETS");
		state.execute(createHql.toString());

		System.out
				.println("Table Storage properties has been updated successfuly");
	}

	/***
	 * Method for update table partition(Add)
	 * 
	 * @throws SQLException
	 **/
	public static void addPartition(String tableName,Map<String, String> partition, String numOfBuckets, String location)throws SQLException {
		StringBuffer createHql = new StringBuffer("ALTER TABLE ")
							.append(tableName)
							.append(" ")
							.append("ADD PARTITION(");
		for (Map.Entry m : partition.entrySet()) 
		{
					createHql.append(m.getKey())
							.append("=")
							.append("'")
							.append(m.getValue())
							.append("'");
		}
					createHql.append(") LOCATION")
							.append("'")
							.append(location)
							.append("'");
		state.execute(createHql.toString());

		System.out.println("Table Storage properties has been updated successfuly");
	}

	/***
	 * Method for update table partition(Rename)
	 * 
	 * @throws SQLException
	 **/
	public static void renamePartition(String tableName,String oldPartition,String newPartition) throws SQLException {
		StringBuffer createHql = new StringBuffer("ALTER TABLE ")
							.append(tableName)
							.append(" ")
							.append(" PARTITION ")
							.append(oldPartition)
							.append(" RENAME TO ")
							.append(newPartition);
		
		state.execute(createHql.toString());

		System.out.println("Partition renamed successfuly");
	}

	/***
	 * Method for update table partition(exchange partition)
	 * 
	 * @throws SQLException
	 **/
	public static void exchangePartition(String tableName1, String tableName2,String partition) throws SQLException {
		StringBuffer createHql = new StringBuffer("ALTER TABLE ")
							.append(tableName2)
							.append("EXCHANGE PARTITION( ")
							.append(partition)
							.append(" WITH TABLE ")
							.append(tableName2);
		state.execute(createHql.toString());

		System.out.println("Partition exchanged successfuly");
	}
	
	
		

	/***
	 * Method for update Column(change column)
	 * 
	 * @throws SQLException
	 **/
	public static void changeColumn(HiveTableDesc column) throws SQLException {
		List<Column> listOfColumn=column.getColumn();
		StringBuffer createHql= new StringBuffer(" ALTER TABLE ");
		for(int i=0;i<=listOfColumn.size();i++) {
			createHql.append(column.getTableName())
					.append(" CHANGE COLUMN ")
					.append(listOfColumn.get(i).getColumnName())
					.append(" ")
					.append(listOfColumn.get(i).getColumnNewName());
					if(StringUtils.isNotBlank(listOfColumn.get(i).getColumnType().getPrecision())&&StringUtils.isNotBlank(listOfColumn.get(i).getColumnType().getScale()))
					{
						createHql.append(listOfColumn.get(i).getColumnType())
								.append("(")
								.append(listOfColumn.get(i).getColumnType().getPrecision())
								.append(",")
								.append(listOfColumn.get(i).getColumnType().getScale())
								.append(")");
					}
					else
					{
						createHql.append(listOfColumn.get(i).getColumnType())
								.append(" COMMENT ")
								.append(" '")
								.append(listOfColumn.get(i).getComment())
								.append("'");
					}
						createHql.append(")")
								.append(" RESTRICT ");
					System.out.println(createHql);
					state.execute(createHql.toString());
		}
		
	}
	
	/***
	 * Method for update Column(replace column)
	 * 
	 * @throws SQLException
	 **/
	public static void replaceColumn(HiveTableDesc column) throws SQLException
	{
		List<Column> listOfColumn=column.getColumn();
		StringBuffer createHql= new StringBuffer(" ALTER TABLE ");
		for(int i=0;i<=listOfColumn.size();i++) {
			createHql.append(column.getTableName())
					.append(" REPLACE COLUMNS (")
					.append(listOfColumn.get(i).getColumnName())
					.append(" ");
					if(StringUtils.isNotBlank(listOfColumn.get(i).getColumnType().getPrecision())&&StringUtils.isNotBlank(listOfColumn.get(i).getColumnType().getScale()))
					{
						createHql.append(listOfColumn.get(i).getColumnType())
						.append("(")
						.append(listOfColumn.get(i).getColumnType().getPrecision())
						.append(",")
						.append(listOfColumn.get(i).getColumnType().getScale())
						.append(")");
					}
					else
					{
						createHql.append(listOfColumn.get(i).getColumnType())
						.append(" COMMENT ")
						.append(" '")
						.append(listOfColumn.get(i).getComment())
						.append(",");
					}
					createHql.deleteCharAt(createHql.length() - 1)
							.append(")")
							.append(" RESTRICT ");
					System.out.println(createHql);
					state.execute(createHql.toString());
		}
	}
		
}
