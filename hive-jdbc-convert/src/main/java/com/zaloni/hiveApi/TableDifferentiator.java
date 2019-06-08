package com.zaloni.hiveApi;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Differentiate between existing table and new table created for update
 * 
 */

public class TableDifferentiator {
	
		
	public static void compareTable(HiveTableDesc existingTable,HiveTableDesc updatedTable) throws SQLException {
		
		if(existingTable.getOwner()!=updatedTable.getOwner())
		{
			System.out.println("The owner '"+ existingTable.getOwner()+"' is changed to '"+ updatedTable.getOwner()+"'");
			HiveUpdate.updateDatabaseOwner(updatedTable.getDatabaseName(), updatedTable.getOwner());
		}
		
			List<Column> existingTableCol = existingTable.getColumn();
			List<Column> updatedTableCol = updatedTable.getColumn();
			
			
			boolean flag=false;
			
			if(existingTableCol.size()>updatedTableCol.size())
			{
				for(int i=0;i<existingTableCol.size();i++)
				{
					for(int j=0;j<updatedTableCol.size();j++)
					{
						if(String.valueOf(existingTableCol.get(i)).equals(String.valueOf(updatedTableCol.get(j))))
						{
							break;
						}
						else
						{
							System.out.println("Replace: exist col " + existingTableCol.get(i) + " update col" + updatedTableCol.get(j));
							flag=true;
						}
						
							
					}
					if(flag==true)
					{
						HiveUpdate.replaceColumn(updatedTable);
					}
				}
			}
			if(existingTableCol.size()<updatedTableCol.size())
			{
				for(int i=0;i<existingTableCol.size();i++)
				{
					for(int j=0;j<updatedTableCol.size();j++)
					{
						if(String.valueOf(existingTableCol.get(i)).equals(String.valueOf(updatedTableCol.get(j))))
						{
							break;
						}
						else if(!String.valueOf(existingTableCol.get(i)).equals(String.valueOf(updatedTableCol.get(j)))) {
							
							System.out.println("vgyuhij");
							HiveTableDesc getTab = new HiveTableDesc();
							
							getTab.setTableName(updatedTable.getTableName());
							getTab.setDatabaseName(updatedTable.getDatabaseName());
							
							Column column= new Column();
							List<Column> columnList= updatedTable.getColumn();
								column.setColumnName(updatedTableCol.get(j).getColumnName());
								DataType dataType=new DataType();
								dataType.setDataType(updatedTableCol.get(j).toString());
								column.setColumnType(dataType);
								column.setComment(updatedTableCol.get(j).getComment());
								
								columnList.add(column);

								getTab.setColumn(columnList);
							HiveUpdate.addColumn(getTab);
							
						}
						else
						{
							
							flag = true;
						}
					}
				}
				if(flag==true)
					HiveUpdate.replaceColumn(updatedTable);
					
			}
			
			else
			{
				for(int i=0;i<existingTableCol.size();i++)
				{
					for(int j=0;j<updatedTableCol.size();j++)
					{
						if(String.valueOf(existingTableCol.get(i)).equals(String.valueOf(updatedTableCol.get(j))))
						{
							break;
						}
						else
						{
							System.out.println("Change : exist col " + existingTable + " update col" + updatedTable);
							flag = true;
						}
					}
				}
				if(flag==true)
					HiveUpdate.changeColumn(updatedTable);
			}
		
		if(existingTable.getTableParameters()!=updatedTable.getTableParameters())
		{
			
			Map<String,String> existingTableMap=existingTable.getTableParameters();
			Map<String,String> updatedTableMap=updatedTable.getTableParameters();

			int count1=0;
			int flag1=0;
			for (Entry m : existingTableMap.entrySet()) {
				for(Entry n:updatedTableMap.entrySet()) 
				{
					if(m.getKey().equals(n.getKey())&&!m.getValue().equals(n.getValue()))
					{
						System.out.println("properties-->'"+m.getKey()+"'='"+m.getValue()+"' changed to--->"+n.getKey()+"="+n.getValue());
						count1++;
						break;
					}
					if(m.getKey().equals(n.getKey())&&m.getValue().equals(n.getValue()))
					{
						count1++;
						break;
					}
					if(!m.getKey().equals(n.getKey())) 
					{
						count1=0;
					}
				}
				if(count1==0)
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
						flag1++;
						System.out.println("properties that are same ---> "+m.getKey()+ " = "+m.getValue()+ " and updated table has "+n.getKey()+" = " +n.getValue());
						break;
					}
					if(!m.getKey().equals(n.getKey()))
					{
						flag1=0;
					}
				}
				if(flag1==0)
				{
					System.out.println("properties added---> "+m.getKey()+"="+m.getValue());
				}
			}
		}
		System.out.println("No change is done");
		System.out.println(existingTable.getTableParameters());
		System.out.println("---------------------------------------------------");
		System.out.println(updatedTable.getTableParameters());
	}

}
