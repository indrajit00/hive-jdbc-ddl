package com.zaloni.hiveApi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;


/**
 * Differentiate between existing table and new table created for update
 * 
 */

public class TableDifferentiator {
	
	public static void compareTable(Table existingTable,Table updatedTable) throws SQLException {
		
		if(existingTable.getTableName()!=updatedTable.getTableName())
		{
			System.out.println("Table Name '"+existingTable.getTableName()+"' has been changed to '"+updatedTable.getTableName()+"'");
		}
		if(existingTable.getTableType()!=updatedTable.getTableType())
		{
			System.out.println("Table Type '"+existingTable.getTableType()+"' has been changed to '"+updatedTable.getTableType()+"'");
		}
		if(existingTable.getDbName()!=updatedTable.getDbName())
		{
			System.out.println("Database name has been changed");
		}
		if(existingTable.getRetention()!=updatedTable.getRetention())
		{
			System.out.println("Retention has been changed");
		}
		if(existingTable.getOwner()!=updatedTable.getOwner())
		{
			System.out.println("The owner '"+ existingTable.getOwner()+"' is changed to '"+ updatedTable.getOwner()+"'");
		}
		if(existingTable.getParameters()!=updatedTable.getParameters())
		{
			
			
			Map<String,String> existingTableMap=existingTable.getParameters();
			Map<String,String> updatedTableMap=updatedTable.getParameters();

			int count=0;
			int flag=0;
			for (Entry m : existingTableMap.entrySet()) {
				for(Entry n:updatedTableMap.entrySet()) 
				{
					if(m.getKey().equals(n.getKey())&&!m.getValue().equals(n.getValue()))
					{
						System.out.println("properties-->'"+m.getKey()+"'='"+m.getValue()+"' changed to--->"+n.getKey()+"="+n.getValue());
						count++;
						break;
					}
					if(m.getKey().equals(n.getKey())&&m.getValue().equals(n.getValue()))
					{
						count++;
						break;
					}
					if(!m.getKey().equals(n.getKey())) 
					{
						count=0;
					}
				}
				if(count==0)
				{
					System.out.println("properties dropped---> "+m.getKey()+"="+m.getValue());
				}
			}
			for(Entry m:updatedTableMap.entrySet())
			{
				for(Entry n:existingTableMap.entrySet())
				{
					if(m.getKey().equals(n.getKey()))
					{
						flag++;
						System.out.println("properties  that are same ---> "+m.getKey()+ " = "+m.getValue()+ " and updated table has "+n.getKey()+" = " +n.getValue());
						break;
					}
					if(!m.getKey().equals(n.getKey()))
					{
						flag=0;
					}
				}
				if(flag==0)
				{
					System.out.println("properties added---> "+m.getKey()+"="+m.getValue());
				}
			}
		}
		System.out.println(existingTable.getParameters());
		System.out.println("---------------------------------------------------");
		System.out.println(updatedTable.getParameters());
	}

}
