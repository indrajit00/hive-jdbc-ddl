package com.zaloni.hiveApi;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import com.zaloni.hiveApi.HiveDbDesc;
import com.zaloni.hiveApi.HiveTableDesc;
import com.zaloni.hiveApi.HiveCreate;



public class ApiTest {

	public static void main(String[] args) throws SQLException {
	
    	//HiveCreate.showTable("zaloni");
		
		/*
		// Create Database
		HiveDbDesc Db=new HiveDbDesc();
		Db.setDatabaseName("New_Database");
		Db.setDbComments("This_is_a_new_Database");
		
		Map<String,String> dbPro=new HashMap<String,String>();
		dbPro.put("Propert1_Name", "Property1_Value");
		dbPro.put("Propert2_Name", "Property2_Value");
		Db.setDbProperties(dbPro);
		
		HiveCreate.createDatabase(Db);
	
		HiveCreate.showDb();
		
		//Create Table
				List<Column> fields = new ArrayList<Column>();
				
				Column field1 = new Column();
				field1.setColumnName("Id");
				DataType dType= new DataType();
				dType.setDataType(HiveDataType.INT);
				field1.setColumnType(dType);
				field1.setComment("This is id field");
				fields.add(field1);
								
				HiveTableDesc table = new HiveTableDesc();
				table.setDatabaseName("default");
				table.setTableName("table7");
				table.setColumn(fields);
		HiveCreate.createTable(table);   			

		HiveUpdate.useDb("zaloni");
		
		HiveCreate.showTable();
		
		HiveUpdate.dropTable("default", "table7");
		
		//Update Database Properties
		Map<String,String> dbPro1=new HashMap<String,String>();
		dbPro1.put("'Date'","'21-02-2019'");
		HiveUpdate.updateDatabaseProperties("New_Database",dbPro1);
		
		HiveUpdate.updateDatabaseOwner("zaloni", "ADMIN"); 
		
		HiveUpdate.updateDatabaseLocation("zaloni", " "); 
		
		HiveUpdate.renameTable("zaloni", "table3008", "table308");    
		
		//Create Table
		List<Column> fields = new ArrayList<Column>();
		
		Column field1 = new Column();
		field1.setColumnName("Id");
		DataType dType= new DataType();
		dType.setDataType(HiveDataType.INT);
		field1.setColumnType(dType);
		field1.setComment("This is id field");
		fields.add(field1);
						
		HiveTableDesc table = new HiveTableDesc();
		table.setDatabaseName("default");
		table.setTableName("table147");
		table.setColumn(fields);
		HiveCreate.createTable(table);   			
		
		Map<String,String> dbPro2=new HashMap<String,String>();
		dbPro2.put("'Date' = ","'21-02-2019'");

		HiveUpdate.updateTableProperties("table_new", dbPro2);		
		

		Map<String,String> dbPro3=new HashMap<String,String>();
		dbPro3.put("Date","22-02-2019");
		
		HiveUpdate.updateSerdeProperties("table147", dbPro3);		
	

		Map<String,String> dbPro4=new HashMap<String,String>();
		dbPro4.put("value1","2000");
		
		HiveUpdate.addPartition("table_new", dbPro4);		
		
		//Change Column name and type of an existing table
		
List<Column> fields = new ArrayList<Column>();
		
		Column field1 = new Column();
		field1.setColumnName("Id");
		field1.setColumnNewName("Tab_ID");
		DataType dType= new DataType();
		dType.setDataType(HiveDataType.INT);
		field1.setColumnType(dType);
		field1.setComment("This is id field");
		fields.add(field1);
		
		HiveTableDesc col = new HiveTableDesc();
		col.setDatabaseName("zaloni");
		col.setTableName("table147");
		col.setColumn(fields);
//		HiveCreate.createTable(col);
		HiveUpdate.changeColumn(col );  		
		
		
		//Replace columns of a table
		
List<Column> fields = new ArrayList<Column>();
		
		Column field1 = new Column();
		field1.setColumnName("Tab_Name");
		DataType dType= new DataType();
		dType.setDataType(HiveDataType.STRING);
		field1.setColumnType(dType);
		field1.setComment("This is id field");
		fields.add(field1);
		
		HiveTableDesc col = new HiveTableDesc();
		col.setDatabaseName("zaloni");
		col.setTableName("table147");
		col.setColumn(fields);
//		HiveCreate.createTable(col);
		HiveUpdate.replaceColumn(col ); 	
	
			//Adding column
List<Column> fields = new ArrayList<Column>();
		
		Column field1 = new Column();
		field1.setColumnName("Tab_City");
		DataType dType= new DataType();
		dType.setDataType(HiveDataType.STRING);
		field1.setColumnType(dType);
		field1.setComment("This is city field");
		fields.add(field1);
		
		HiveTableDesc col = new HiveTableDesc();
		col.setDatabaseName("zaloni");
		col.setTableName("table147");
		col.setColumn(fields);
//		HiveCreate.createTable(col);
		HiveUpdate.addColumn(col);
	
		
		Map<String,String> dbPro4=new HashMap<String,String>();
		dbPro4.put("CONATCT","7896130");
		
		HiveUpdate.addPartition("PART_TAB", dbPro4);	
	
/*		StringBuffer createHql = new StringBuffer("CREATE TABLE PART_TAB(ID INT, NAME STRING, ADD STRING) PARTITIONED BY (CONATCT STRING) ");
		HiveQueryExecutor.execute(createHql.toString());
		System.out.println("PARTITON TAB CREATED");		*/

/*		Map<String, String> oldpart = new HashMap<String,String>();
		oldpart.put("CONATCT", "7896130");
		
		Map<String, String> newpart = new HashMap<String, String>();
		newpart.put("CONATCT", "789456123");
		
		HiveUpdate.renamePartition("PART_TAB", oldpart, newpart);	
		
		StringBuffer createHql = new StringBuffer("CREATE TABLE PART_TAB3
		(ID INT, NAME STRING, ADD STRING) PARTITIONED BY (CITY STRING) ");
		HiveQueryExecutor.execute(createHql.toString());
		System.out.println("PARTITON TAB CREATED");		
	
		Map<String, String> drpart = new HashMap<String, String>();
		drpart.put("CONATCT", "78894512");		
		HiveUpdate.dropPartition("PART_TAB", drpart);		
		
		Map<String,String> dbPro4=new HashMap<String,String>();
		dbPro4.put("CITY","JAIPUR");
		
		HiveUpdate.addPartition("PART_TAB3", dbPro4);
		
		Map<String, String> partff = new HashMap<String, String>();
		partff.put("CITY", "JAIPUR");		
		HiveUpdate.updatePartitionFileFormat("PART_TAB3", partff, "TEXTFILE");
		
		HiveUpdate.updatePartitionLocation("default","PART_TAB3", partff, "hdfs://hdpdev-n1.zalonilabs.com:8020/user/zaloni/t1");
	
		HiveUpdate.updateDatabaseLocation("new_database", "/user/zaloni/t1"); 
		
		//Adding column
List<Column> fields = new ArrayList<Column>();
	
	Column field1 = new Column();
	field1.setColumnName("Tab_New");
	DataType dType= new DataType();
	dType.setDataType(HiveDataType.STRING);
	field1.setColumnType(dType);
	field1.setComment("This is new field");
	fields.add(field1);
	
	HiveTableDesc col = new HiveTableDesc();
	col.setDatabaseName("zaloni");
	col.setTableName("table147");
	col.setColumn(fields);
HiveCreate.createTable(col);
	HiveUpdate.addColumn(col);
	*/
    /////////////////////////////////////////////////////////////////////////////////////
    	
	/////////////////////////////////////////////////////////////////////////////
	Table existingTable = HiveMetadataExtractor.getTable("zaloni", "table12");
	//System.out.println(existingTable);
	
	Table updatedTableRequest = existingTable.deepCopy();
	//System.out.println(updatedTableRequest.getSd().getCols());
	//System.out.println("#############################");
	FieldSchema newField = new FieldSchema();
	newField.setName("weight");
	newField.setType("int");
	
	FieldSchema newField2 = new FieldSchema();
	newField2.setName("height");
	newField2.setType("decimal(5,2)");
	FieldSchema partition=new FieldSchema();
	partition.setName("Member_name");
	partition.setType("String");
	

	updatedTableRequest.getSd().getCols().add(newField);
	updatedTableRequest.getSd().getCols().add(newField2);
	//updatedTableRequest.getSd().getCols().remove(0);
	
	System.out.println("################################################");
	System.out.println();
	updatedTableRequest.setTableType("External");
	updatedTableRequest.setTableName("newTableName");
	Map<String,String> tablePro=new HashMap<String,String>();
	tablePro.put("key1", "value1");
	tablePro.put("totalSize", "22");
	tablePro.put("numRows", "1");
	tablePro.put("rawDataSize","10");
	
	updatedTableRequest.setParameters(tablePro);
	TableDifferentiator.compareTable(existingTable, updatedTableRequest);
	//System.out.println("New table description to be updated - " + updatedTableRequest);

	
	//////////////////////////////////////////////////////////////////////////////////////
	// existingTable Vs updatedTableRequest
	}
}
