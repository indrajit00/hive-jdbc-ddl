package com.zaloni.hiveApi;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Table;


public class MetaDataOperationsExecutor {

	public static void executeTask(String type) throws JsonParseException, JsonMappingException, IOException, SQLException {
		
		
		
		 HiveTableDesc hiveTableRequest = new ObjectMapper().readValue(new File("C:\\Users\\rmour\\Desktop\\hack\\jsontest.json"), HiveTableDesc.class);
	        
	        System.out.println(" "+ hiveTableRequest); 
	        
	        ResultSet show_Tab = HiveQueryExecutor.executeQuery("show tables in " + hiveTableRequest.getDatabaseName());
	        List<String> existingTables = new ArrayList<>();
			while (show_Tab.next()) {
				System.out.println(show_Tab.getString(1));
				
				existingTables.add(show_Tab.getString(1));
				
			}
	
			boolean isExists = false;
			
			for(int i=0;i<existingTables.size();i++)
			{
				
			
				if(String.valueOf(existingTables.get(i)).equalsIgnoreCase(hiveTableRequest.getTableName())) {
					isExists = true;
					System.out.println("Table already exists");
					break;	
					
				}
				else
				{
					isExists = false;
//					System.out.println("else is printed");				
				}
			
			}
				if(isExists == true) {
					System.out.println("yujbn");
					System.out.println("Database Name "+ hiveTableRequest.getDatabaseName()+" "+hiveTableRequest.getTableName());
					if(hiveTableRequest.isDeleted() == true) {
						System.out.println("sjhjshdjhsdhjshdjshdjshdjsh");
						HiveUpdate.dropTable(hiveTableRequest.getDatabaseName(), hiveTableRequest.getTableName(), "CASCADE");
						
					}
					if(hiveTableRequest.isFetchTable() == true) {
						System.out.println("ghjkl");
					
						HiveTableOperations.fetchTableDetails(hiveTableRequest.getDatabaseName(), hiveTableRequest.getTableName());
					
					}
					
					HiveTableDesc existingTable = HiveTableOperations.fetchTableDetails(hiveTableRequest.getDatabaseName(), hiveTableRequest.getTableName());
					System.out.println("existing " + existingTable);
					
					HiveTableDesc updatedTableRequest = new ObjectMapper().readValue(new File("C:\\Users\\rmour\\Desktop\\hack\\jsontest.json"), HiveTableDesc.class);
					System.out.println("update " + updatedTableRequest);
					TableDifferentiator.compareTable(existingTable, updatedTableRequest);	
			
				}
				else {
					System.out.println("Execute code for create");
					HiveTableOperations.createTable(hiveTableRequest);
				}
				
		

		}

		public static void executeTask(String type, HiveTableDesc hiveTable) throws JsonParseException, JsonMappingException, IOException, SQLException {
			
			if(type == "CREATE") {
				if(hiveTable == null) {
					System.out.println("PLease provide Location for JSON");
				}
				else
				{
					HiveTableOperations.createTable(hiveTable);
				}
			}
			if(type == "UPDATE") {
				if(hiveTable == null) {
					System.out.println("Please provide Location for JSON to update");
				}
				else
				{

					HiveTableDesc existingTable = HiveTableOperations.fetchTableDetails(hiveTable.getDatabaseName(), hiveTable.getTableName());
					System.out.println("existing " + existingTable);
					
					HiveTableDesc updatedTableRequest = new ObjectMapper().readValue(new File("C:\\Users\\rmour\\Desktop\\hack\\jsontest.json"), HiveTableDesc.class);
					System.out.println("update " + updatedTableRequest);
					TableDifferentiator.compareTable(existingTable, updatedTableRequest);	
			
				}
			}
			if(type == "FETCH") {
				
				HiveTableOperations.fetchTableDetails(hiveTable.getDatabaseName(), hiveTable.getTableName());
			}
			if(type == "DELETE") {
				HiveUpdate.dropTable(hiveTable.getDatabaseName(), hiveTable.getTableName());
			}
		
		}
	}
