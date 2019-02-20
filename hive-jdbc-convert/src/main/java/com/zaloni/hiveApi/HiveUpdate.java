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
 *This is a Utility class for updating database,table
 */
public class HiveUpdate {

	private static final String driver = "org.apache.hive.jdbc.HiveDriver";
	private static Statement state;
	private static Connection connect;
	
	private HiveUpdate(){}//Constructor
	
	/***
	 * Method for Dropping a Database
	 * 
	 * @throws SQLException
	 **/
	/**
	 * @param databaseName name of the database
	 * @throws SQLException
	 */
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
	/**
	 * @param databaseName name of the database
	 * @throws SQLException
	 */
	public static void useDb(String databaseName) throws SQLException {
		
		state.execute("use " + databaseName);
		System.out.println(databaseName+ " is now being used");

	}
	
	/***
	 * Method to update database properties
	 * 
	 * @throws SQLException
	 **/
	/**
	 * @param databaseName name of the database
	 * @param databaseProperties mapped value of database properties of String type
	 * @throws SQLException
	 */
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
	/**
	 * @param databaseName name of the database
	 * @param role user/role
	 * @throws SQLException
	 */
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
	/**
	 * @param databaseName name of the database
	 * @param location location to which the database will be stored
	 * @throws SQLException
	 */
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
	 * Method for Dropping a Table
	 * 
	 * @throws SQLException
	 **/
	/**
	 * @param tableName name of the table
	 * @throws SQLException
	 */
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
	/**
	 * @param DbName name of the database consisting the table
	 * @param TableName name of the table
	 * @param NewTableName
	 * @throws SQLException
	 */
	public static void renameTable(String DbName ,String TableName , String NewTableName)throws SQLException {
		StringBuffer createHql= new StringBuffer();
					createHql.append(" ALTER TABLE ")
							.append(DbName)
							.append(".")
							.append(TableName)
							.append(" ");
					createHql.append("RENAME TO ")
							.append(DbName)
							.append(".")
							.append(NewTableName);
		state.execute(createHql.toString());
		System.out.println("The "+TableName+" is renamed with "+NewTableName);

		}
	
	/***
	 * Method for update table properties
	 * 
	 * @throws SQLException
	 **/
	/**
	 * @param tableName name of the table
	 * @param tableProperties mapped value of table properties of String type
	 * @throws SQLException
	 */
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
	/**
	 * @param tableName name of the table
	 * @param serdeProperties mapped value of the table serde properties of String type
	 * @throws SQLException
	 */
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
	/**
	 * @param tableName name of the table
	 * @param storageProperties mapped values of storage properties of String type
	 * @param numOfBuckets number of buckets
	 * @throws SQLException
	 */
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
	/**
	 * @param tableName name of the table
	 * @param partition name of the partition
	 * @param numOfBuckets number of buckets
	 * @param location table location
	 * @throws SQLException
	 */
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
	/**
	 * @param tableName name of the table
	 * @param oldPartition 
	 * @param newPartition
	 * @throws SQLException
	 */
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
	/**
	 * @param tableName1 name of the table 1
	 * @param tableName2 name of the table 2
	 * @param partition name of the partition
	 * @throws SQLException
	 */
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
	/**
	 * @param column object containing column name, column data type
	 * @throws SQLException
	 */
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
	/**
	 * @param column object of HiveTabletype containing column name, column data type
	 * @throws SQLException
	 */
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
	
	
	/***
	* Method for Drop Partition
	* 
	* @throws SQLException 
	*/

	/**
	 * @param TableName name of the table
	 * @param partition name of the partition
	 * @throws SQLException
	 */
	public static void dropPartition(String TableName, Map<String, String> partition) throws SQLException {

	StringBuffer createHql= new StringBuffer("ALTER TABLE ")
						.append(TableName)
						.append(" DROP IF EXISTS PARTITION(");
	for (Map.Entry m : partition.entrySet()) {
				createHql.append(m.getKey() + "=" + "'" + m.getValue() + "'");
	}
				createHql.append(") PURGE");
	state.execute(createHql.toString());
	System.out.println(TableName+" Partition is dropped successfully");

	}
	
	/***
	* Method for Alter Partition FileFormat
	* 
	* @throws SQLException 
	*/

	public static void updatePartitionFileFormat(String TableName , Map<String, String> partition  , String FileFormat) {

	StringBuffer createHql = new StringBuffer("ALTER TABLE ")
						.append(TableName)
						.append(" PARTTION(");
	for (Map.Entry m : partition.entrySet()) {
				createHql.append(m.getKey() + "=" + "'" + m.getValue() + "'");
	}
				createHql.append(" SET FILEFORMAT ")
						.append(FileFormat);
	}
	
	
	/***
	* Method for Alter Partition Location
	* 
	* @throws SQLException 
	*/

	/**
	 * @param TableName name of the table
	 * @param partition name of the partition
	 * @param Location location of the table
	 */
	public static void updatePartitionLocation(String TableName , Map<String, String> partition  , String Location) {

	StringBuffer createHql = new StringBuffer("ALTER TABLE ")
						.append(TableName)
						.append(" PARTTION(");
	for (Map.Entry m : partition.entrySet()) {
				createHql.append(m.getKey() + "=" + "'" + m.getValue() + "'");
	}
				createHql.append(" SET LOCATOIN( ")
						.append(Location)
						.append(" )");

	}

	
	
}
