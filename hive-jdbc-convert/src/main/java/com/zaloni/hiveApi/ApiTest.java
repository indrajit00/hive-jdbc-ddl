
package com.zaloni.hiveApi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zaloni.hiveApi.HiveDbDesc;
import com.zaloni.hiveApi.HiveTableDesc;
import com.zaloni.hiveApi.HiveUtility;


public class ApiTest {

	public static void main(String[] args) throws SQLException {
		HiveUtility.checkCon();

		HiveUtility.showDb();
		
		
		/*
		HiveDbDesc Db=new HiveDbDesc();
		Db.setDatabaseName("New_Database");
		Db.setDbComments("This_is_a_new_Database");
		
		Map<String,String> dbPro=new HashMap<String,String>();
		dbPro.put("Propert1_Name", "Property1_Value");
		dbPro.put("Propert2_Name", "Property2_Value");
		Db.setDbProperties(dbPro);
		
		HiveUty.createDb(Db);
	*/
		
		
		
		//Create Table
		List<TableFields> fields = new ArrayList<TableFields>();
		
		TableFields field1 = new TableFields();
		field1.setColumnName("Id");
		DataType dType= new DataType();
		dType.setDataType(HiveDataType.DECIMAL);
		dType.setPrecision("10");
		dType.setScale("4");
		field1.setColumnType(dType);
		field1.setComment("This is id field");
		fields.add(field1);
		
		
		
		
		HiveTableDesc table = new HiveTableDesc();
		table.setDatabaseName("zaloni");
		table.setTableName("table38");
		table.setColumn(fields);
		HiveUtility.createTable(table);
	
		
		
		/*HiveTableDesc tDesc=new HiveTableDesc();
		
		tDesc.getComments("This is comment");
		tDesc.getDatabaseName("Zaloni");
		tDesc.getTableName("newTable");
		HiveUty.createTable(tDesc,fieldDetails);*/
		//HiveUty.rename_tb("employee", "new_employee");
	
		//HiveUty.set_Tb_property("tab5","new_comment","this is new table comment");
		//HiveUty.set_db_ property("zaloni", "comment", "this is  new db");
		
		/*
		HiveTableDesc tableProperties=new HiveTableDesc();
		tableProperties.setTableName("newtable");
		HiveUty.tableDescription(tableProperties);*/
		
		/*HiveDbDesc hiveDb=new HiveDbDesc();
		hiveDb.setDatabaseName("zaloni");
		Map<String,String> dbPro=new HashMap<String,String>();
		dbPro.put("Property1","property_Value1");
		dbPro.put("property2","property_value2");
		hiveDb.setDbProperties(dbPro);
		HiveUtility.setDbProperty(hiveDb);*/
	}

}
