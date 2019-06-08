package com.zaloni.hiveApi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

public class TableDetailsExtractor {

	public static HiveTableDesc describeTable(String database, String tableName) throws SQLException {
        String cmd = "DESC FORMATTED ".concat(database).concat(".").concat(tableName);
        ResultSet rs = HiveConnectionProvider.getStatement().executeQuery(cmd);

        rs.next(); // Start resultSet pointer
        HiveTableDesc hiveTable= new HiveTableDesc();
        hiveTable.setColumn(getColumns(rs));
//        List<List<String>> columnDefinitionList = getColumns(rs);
//        System.out.println("Columns :: " + columnDefinitionList);
        List<List<String>> partitionDefinitionList = getPartitions(rs);
        System.out.println("Partitions :: " + partitionDefinitionList);
        Map<String,Object> detailTableDescription = getDetailTableDescription(rs);
        System.out.println("Detail Table Description :: " + detailTableDescription);
        Map<String,Object> storageInformation = getStorageInformation(rs);
        System.out.println("Storage Information :: " + storageInformation);
        
        return hiveTable;
		  
}

private static Map<String, Object> getDetailTableDescription(ResultSet rs) throws SQLException {
        Map<String,Object> detailTableDescMap = new HashMap<>();
        Map<String,String> tableParametersMap = new HashMap<>();
        while (rs.next()) {
             if (StringUtils.isNotBlank(rs.getString(1)) && rs.getString(1).startsWith("#")) {
                  break;
             }

             if (StringUtils.isNotBlank(rs.getString(1)) && StringUtils.isNotBlank(rs.getString(2))) {
                  detailTableDescMap.put(rs.getString(1).trim(),rs.getString(2).trim());
             }

             if (StringUtils.isBlank(rs.getString(1)) && StringUtils.isNotBlank(rs.getString(2)) && StringUtils
                       .isNotBlank(rs.getString(3))) {
                  tableParametersMap.put(rs.getString(2).trim(), rs.getString(3).trim());
             }

        }
        if(MapUtils.isNotEmpty(tableParametersMap)) {
             //System.out.println("Table Parameters : " + tableParametersMap);
             detailTableDescMap.put("Table Parameters:", tableParametersMap);
        }
        return detailTableDescMap;
   }

   private static Map<String, Object> getStorageInformation(ResultSet rs) throws SQLException {
        Map<String,Object> storageInformationMap = new HashMap<>();
        Map<String,String> storageDescParamMap = new HashMap<>();
        while (rs.next()) {
             if (StringUtils.isNotBlank(rs.getString(1)) && rs.getString(1).startsWith("#")) {
                  break;
             }

             if (StringUtils.isNotBlank(rs.getString(1)) && StringUtils.isNotBlank(rs.getString(2))) {
                  storageInformationMap.put(rs.getString(1).trim(),rs.getString(2).trim());
             }

             if (StringUtils.isBlank(rs.getString(1)) && StringUtils.isNotBlank(rs.getString(2)) && StringUtils
                       .isNotBlank(rs.getString(3))) {
                  storageDescParamMap.put(rs.getString(2).trim(), rs.getString(3).trim());
             }

        }
        if(MapUtils.isNotEmpty(storageDescParamMap)) {
             //System.out.println("Storage Desc Params: " + tableParametersMap);
             storageInformationMap.put("Storage Desc Params:", storageDescParamMap);
        }
        return storageInformationMap;
   }

   private static List<Column> getColumns(ResultSet rs) throws SQLException {
        //System.out.println("Getting columns from -- " + rs.getString(1) + "==" + rs.getString(2) + "==" + rs.getString(3));
        List<List<String>> columnDefinitionList = new ArrayList<>();
        List<Column> columnList=new ArrayList<Column>();
        if (rs.getString(1).startsWith("# col_name")) {
             while (rs.next()) {
                  //System.out.println("Getting columns from -- " + rs.getString(1) + "==" + rs.getString(2) + "==" + rs.getString(3));
                  if (StringUtils.isNotBlank(rs.getString(1)) && rs.getString(1).startsWith("#")) {
                       break;
                  }
                  if (StringUtils.isNotBlank(rs.getString(1)) && StringUtils.isNotBlank(rs.getString(2))) {
//                       List<String> columnData = new ArrayList<>();
//                       columnData.add(rs.getString(1).trim());
//                       columnData.add(rs.getString(2).trim());
//                       columnData.add(rs.getString(3).trim());
//                       
                       
                    	   Column column= new Column();
                    	   column.setColumnName(rs.getString(1).trim());
                    	   DataType dataType=new DataType();
                    	   dataType.setDataType(rs.getString(2).trim());
                    	   column.setColumnType(dataType);
                    	   column.setComment(rs.getString(3).trim());
                    	   
                    	   columnList.add(column);
                       
                       
//                       columnDefinitionList.add(columnData);
                  }
             }

        }
        
        
       
        return columnList;
   }

   /**
    * Get the partitions out from the description
    * Sample :<br/>
    * # Partition Information    null           null <br/>
    * # col_name                 data_type      comment <br/>
    *                            null           null <br/>
    * join_year                  int            system generated <br/>
    *                            null           null <br/>
    *
    * @param rs
    * @return
    * @throws SQLException
    */
   private static List<List<String>> getPartitions(ResultSet rs) throws SQLException {
        //System.out.println(="Getting partitions from -- " + rs.getString(1) + "==" + rs.getString(2) + "==" + rs.getString(3));
        List<List<String>> partitionDefinitionList = new ArrayList<>();
        if (rs.getString(1).startsWith("# Partition Information")) {
             rs.next(); //Skipping this line from the description -> # Partition Information
             while (rs.next()) {
                  //System.out.println("Getting partitions from -- " + rs.getString(1) + "==" + rs.getString(2) + "==" + rs.getString(3));
                  if (StringUtils.isNotBlank(rs.getString(1)) && rs.getString(1).startsWith("#")) {
                       break;
                  }
                  if (StringUtils.isNotBlank(rs.getString(1)) && StringUtils.isNotBlank(rs.getString(2))) {
                       List<String> partitionData = new ArrayList<>();
                       partitionData.add(rs.getString(1).trim());
                       partitionData.add(rs.getString(2).trim());
                       partitionData.add(rs.getString(3).trim());
                       partitionDefinitionList.add(partitionData);
                  }
             }

        }
        return partitionDefinitionList;
   }
}
