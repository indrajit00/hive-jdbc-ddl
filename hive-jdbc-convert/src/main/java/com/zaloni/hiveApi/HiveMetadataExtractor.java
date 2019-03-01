package com.zaloni.hiveApi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveMetadataExtractor {
	
	
	public static Table getTable(String database, String table) throws SQLException {
		return getTableFromStringDesc(database, table);
	}
	
	private static Table getTableFromStringDesc(String database, String tableName) throws SQLException {
        String cmd = "DESC FORMATTED ".concat(database).concat(".").concat(tableName);
        ResultSet rs = HiveConnectionProvider.getStatement().executeQuery(cmd);

        rs.next(); // Start resultSet pointer
        List<List<String>> columnDefinitionList = getColumns(rs);
        System.out.println("Columns :: " + columnDefinitionList);
        List<List<String>> partitionDefinitionList = getPartitions(rs);
        System.out.println("Partitions :: " + partitionDefinitionList);
        Map<String, Object> detailTableDescription = getDetailTableDescription(rs);
        System.out.println("Detail Table Description :: " + detailTableDescription);
        Map<String, Object> storageInformation = getStorageInformation(rs);
        System.out.println("Storage Information :: " + storageInformation);
        long createTimeLong = getDate(detailTableDescription.get("CreateTime:")).getTime();
        int createTime = (int) createTimeLong / 1000;

        int lastAccessTime = 0;
        if (!"UNKNOWN".equalsIgnoreCase((String) detailTableDescription.get("LastAccessTime:"))) {
             long lastAccessTimeLong = getDate(detailTableDescription.get("LastAccessTime:")).getTime();
             lastAccessTime = (int) lastAccessTimeLong / 1000;
        }

        String retentionString = (String) detailTableDescription.get("Retention:");
        int retention = StringUtils.isNotBlank(retentionString) && StringUtils.isNumeric(retentionString) ?
                  Integer.parseInt(retentionString) : 0;

        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(columnDefinitionList)) {
             for (List<String> columnData : columnDefinitionList) {
                  FieldSchema fieldSchema =
                            new FieldSchema(columnData.get(0), columnData.get(1), columnData.get(2));
                  fieldSchemaList.add(fieldSchema);
             }
        }

        boolean compressed = "YES".equalsIgnoreCase((String) storageInformation.get("Compressed:"));

        String numBucketString = (String) storageInformation.get("Num Buckets:");
        int numBuckets = StringUtils.isNotBlank(numBucketString) && StringUtils.isNumeric(numBucketString) ?
                  Integer.parseInt(numBucketString) : 0;

        SerDeInfo serdeInfo = new SerDeInfo(null, (String) storageInformation.get("SerDe Library:"),
                  (Map<String, String>) storageInformation.get("Storage Desc Params:"));

        String bucketColumnsString = (String) storageInformation.get("Bucket Columns:");
        List<String> bucketColumnList = new ArrayList<>();
        if (StringUtils.isNotBlank(bucketColumnsString) && !bucketColumnsString.equals("[]")) {
             bucketColumnsString = bucketColumnsString
                       .substring(bucketColumnsString.indexOf("["), bucketColumnsString.lastIndexOf("]"));
             bucketColumnList = Arrays.asList(bucketColumnsString.split(","));
        }

        String sortColumnsString = (String) storageInformation.get("Sort Columns:");
        List<Order> sortColumnList = new ArrayList<>();
        if (StringUtils.isNotBlank(sortColumnsString) && !sortColumnsString.equals("[]")) {
             sortColumnsString = sortColumnsString
                       .substring(sortColumnsString.indexOf("["), sortColumnsString.lastIndexOf("]"));
             //sortColumnList = Arrays.asList(sortColumnsString.split(","));
             //TODO: Need to work on  sort columns
        }

        StorageDescriptor storageDescriptor = new StorageDescriptor(fieldSchemaList,
                  (String) detailTableDescription.get("Location:"),
                  (String) storageInformation.get("InputFormat:"),
                  (String) storageInformation.get("OutputFormat:"), compressed, numBuckets, serdeInfo,
                  bucketColumnList,
                  sortColumnList, null);
        // TODO: Set optional properties to Storage Descriptor like skew
        //SkewedInfo skewedInfo = new SkewedInfo();
        //boolean storedAsSubDirectories;
        //storageDescriptor.setStoredAsSubDirectories();
        //storageDescriptor.setSkewedInfo();

        //Partition details
        List<FieldSchema> partitionFieldSchemaList = new ArrayList<>();
        boolean partitionKeysIsSet = false;
        if (CollectionUtils.isNotEmpty(partitionDefinitionList)) {
             for (List<String> partitionData : partitionDefinitionList) {
                  FieldSchema fieldSchema =
                            new FieldSchema(partitionData.get(0), partitionData.get(1), partitionData.get(2));
                  partitionFieldSchemaList.add(fieldSchema);
             }
             partitionKeysIsSet = true;
        }
        Map<String, String> tableParameters =
                  (Map<String, String>) detailTableDescription.get("Table Parameters:");

        Table table= new Table(tableName, database, (String) detailTableDescription.get("Owner:"),
                  createTime, lastAccessTime, retention, storageDescriptor, partitionFieldSchemaList,
                  tableParameters, null, null, (String) detailTableDescription.get("Table Type:"));

        System.out.println("Table decription is -- " + table);
        return table;
   }

   /**
    * Input string is in - EEE MMM dd HH:mm:ss zzz yyyy Format. THis is is format used by Java Date toString()
    * Example : Fri Feb 22 16:11:44 IST 2019
    * @param dateString
    * @return
    */
   private static Date getDate(Object dateString) {
        DateFormat dateFormat = new SimpleDateFormat(
                  "EEE MMM dd HH:mm:ss zzz yyyy");
        try {
             return dateFormat.parse((String)dateString);
        } catch (ParseException e) {
             return new Date();
        }
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

   private static List<List<String>> getColumns(ResultSet rs) throws SQLException {
        //System.out.println("Getting columns from -- " + rs.getString(1) + "==" + rs.getString(2) + "==" + rs.getString(3));
        List<List<String>> columnDefinitionList = new ArrayList<>();
        if (rs.getString(1).startsWith("# col_name")) {
             while (rs.next()) {
                  //System.out.println("Getting columns from -- " + rs.getString(1) + "==" + rs.getString(2) + "==" + rs.getString(3));
                  if (StringUtils.isNotBlank(rs.getString(1)) && rs.getString(1).startsWith("#")) {
                       break;
                  }
                  if (StringUtils.isNotBlank(rs.getString(1)) && StringUtils.isNotBlank(rs.getString(2))) {
                       List<String> columnData = new ArrayList<>();
                       columnData.add(rs.getString(1).trim());
                       columnData.add(rs.getString(2).trim());
                       columnData.add(rs.getString(3).trim());
                       columnDefinitionList.add(columnData);
                  }
             }

        }
        return columnDefinitionList;
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
