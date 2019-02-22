
package com.zaloni.hiveApi;




import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zaloni.hiveApi.HiveDbDesc;
import com.zaloni.hiveApi.HiveTableDesc;
import com.zaloni.hiveApi.HiveCreate;



public class ApiTest {

	public static void main(String[] args) throws SQLException {
	
		HiveCreate.showDb();
		
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
		HiveUpdate.changeColumn(col );  		*/
		
		
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
		
	}

}
