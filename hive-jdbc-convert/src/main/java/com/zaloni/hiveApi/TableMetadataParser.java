package com.zaloni.hiveApi;

import java.io.IOException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TableMetadataParser {

    private static final String TEMPORARY_STR = "temporary";
    private static final String TABLE_TYPE_STR = "tableType";
    private static final String VIEW_EXPANDED_TEXT_STR = "viewExpandedText";
    private static final String VIEW_ORIGINAL_TEXT_STR = "viewOriginalText";
    private static final String RETENTION_STR = "retention";
    private static final String LAST_ACCESS_TIME_STR = "lastAccessTime";
    private static final String CREATE_TIME_STR = "createTime";
    private static final String OWNER_STR = "owner";
    private static final String DB_NAME_STR = "dbName";
    private static final String TABLE_NAME_STR = "tableName";
    private static final String PARTITION_KEYS_STR = "partitionKeys";
    private static final String NUM_BUCKETS_KEY = "numBuckets";
    private static final String COMPRESSED_STR = "compressed";
    private static final String OUTPUT_FORMAT_STR = "outputFormat";
    private static final String INPUT_FORMAT_STR = "inputFormat";
    private static final String LOCATION_STR = "location";
    private static final String PARAMETERS_KEY = "parameters";
    private static final String BUCKET_COLS_KEY = "bucketCols";
    private static final String SERIALIZATION_LIB_KEY = "serializationLib";
    private static final String SERDE_INFO_KEY = "serdeInfo";
    private static final String SD_KEY = "sd";
    private static final String COMMENT_STR = "comment";
    private static final String TYPE_STR = "type";
    private static final String NAME_STR = "name";
    private static final String COLUMNS_STR = "columns";
    private static final String TABLE_INFO_KEY = "tableInfo";
    
    

    public static Table populateHiveTableFromJson(String tableDescStr) throws JSONException {
        JSONObject tableDescObject = new JSONObject(tableDescStr);
        JSONObject tableObject = tableDescObject.getJSONObject(TABLE_INFO_KEY);

        List<String> partitionNameList = new ArrayList<>();

        // extract partition schema fields
        List<FieldSchema> partitionSchemaList = extractPartitionSchemaFields(tableObject);

        if(CollectionUtils.isNotEmpty(partitionSchemaList)){
            for(FieldSchema fs : partitionSchemaList){
                partitionNameList.add(fs.getName());
            }
        }

        // Extract Fields
        List<FieldSchema> fieldSchemaList = extractTableFields(tableDescObject,partitionNameList);

        // Extract Storage Descriptor
        StorageDescriptor sd = extractStorageDescriptor(tableObject, fieldSchemaList);
        
        //Extract parameters to the table
        Map<String, String> tableParameters = new HashMap<>();
        if (tableObject != null && tableObject.get(PARAMETERS_KEY) != null) {
            tableParameters = readMapObjectFromJson(tableObject.getJSONObject(PARAMETERS_KEY).toString());
        }

        Table table = new Table();
        table.setTableName(tableObject.getString(TABLE_NAME_STR));
        table.setDbName(tableObject.getString(DB_NAME_STR));
        table.setOwner(tableObject.getString(OWNER_STR));
        table.setCreateTime(tableObject.getInt(CREATE_TIME_STR));
        table.setLastAccessTime(tableObject.getInt(LAST_ACCESS_TIME_STR));
        if (tableObject.has(RETENTION_STR)) {
            table.setRetention(tableObject.getInt(RETENTION_STR));
        }
        table.setSd(sd);
        table.setPartitionKeys(partitionSchemaList);
        table.setParameters(tableParameters);
        if (tableObject.has(VIEW_ORIGINAL_TEXT_STR)) {
            table.setViewOriginalText(tableObject.getString(VIEW_ORIGINAL_TEXT_STR));
        }
        if (tableObject.has(VIEW_EXPANDED_TEXT_STR)) {
            table.setViewExpandedText(tableObject.getString(VIEW_EXPANDED_TEXT_STR));
        }
        table.setTableType(tableObject.getString(TABLE_TYPE_STR));
        if (tableObject.has(TEMPORARY_STR)) {
            table.setTemporary(tableObject.getBoolean(TEMPORARY_STR));
        }
        return table;
    }

    public static Database getDatabaseMetadata(String databaseDescStr) throws JSONException {
        JSONObject databaseDescObject = new JSONObject(databaseDescStr);
        Database hiveDatabase = new Database(databaseDescObject.getString("database"), StringUtils.EMPTY,
                  databaseDescObject.getString("location"), null);
        hiveDatabase.setOwnerName(databaseDescObject.getString("owner"));
        hiveDatabase.setOwnerNameIsSet(true);
        return hiveDatabase;
    }

    private static Map<String, String> readMapObjectFromJson(String jsonStr) throws JSONException {
        try {
            return new ObjectMapper().readValue(jsonStr, HashMap.class);
        } catch (IOException e) {
            throw new JSONException(e);
        }
    }

    private static List<FieldSchema> extractTableFields(JSONObject tableDescObject, List<String> partitionNames) throws JSONException {
        List<FieldSchema> fieldSchemaList = null;
        if (tableDescObject != null && tableDescObject.getJSONArray(COLUMNS_STR) != null) {
            JSONArray columnArray = tableDescObject.getJSONArray(COLUMNS_STR);
            fieldSchemaList = new ArrayList<>(columnArray.length());
            for (int i = 0; i < columnArray.length(); i++) {
                String comment = columnArray.getJSONObject(i).has(COMMENT_STR) ?
                    (String) columnArray.getJSONObject(i).get(COMMENT_STR) : "";
                String fieldName = columnArray.getJSONObject(i).getString(NAME_STR);
                if(partitionNames.contains(fieldName)){
                    continue;
                }
                FieldSchema fieldSchema = new FieldSchema(fieldName,
                    columnArray.getJSONObject(i).getString(TYPE_STR), comment);
                fieldSchemaList.add(fieldSchema);
            }
        }
        return fieldSchemaList;
    }

    private static StorageDescriptor extractStorageDescriptor(JSONObject tableObject,
        List<FieldSchema> fieldSchemaList) throws JSONException {

        StorageDescriptor sd = null;
        if (tableObject != null && tableObject.get(SD_KEY) != null) {
            JSONObject sdJSONObject = tableObject.getJSONObject(SD_KEY);
            SerDeInfo serDeInfo = null;
            if (sdJSONObject != null && sdJSONObject.has(SERDE_INFO_KEY)
                && sdJSONObject.getJSONObject(SERDE_INFO_KEY) != null) {
                JSONObject serdeInfoObject = sdJSONObject.getJSONObject(SERDE_INFO_KEY);

                Map<String, String> parameters = new HashMap<>();
                if (serdeInfoObject != null && serdeInfoObject.has(PARAMETERS_KEY)
                    && serdeInfoObject.getJSONObject(PARAMETERS_KEY) != null) {
                    parameters = readMapObjectFromJson(serdeInfoObject.getJSONObject(PARAMETERS_KEY).toString());
                }
                String serdeName = (serdeInfoObject.has(NAME_STR) && serdeInfoObject.getString(NAME_STR) != null) ?
                    serdeInfoObject.getString(NAME_STR) : "";

                serDeInfo = new SerDeInfo(serdeName,
                    serdeInfoObject.getString(SERIALIZATION_LIB_KEY), parameters);
            }

            List<String> bucketColumns = new ArrayList<String>();
            if (sdJSONObject != null && sdJSONObject.has(BUCKET_COLS_KEY)
                && sdJSONObject.getJSONArray(BUCKET_COLS_KEY) != null) {
                for (int i = 0; i < sdJSONObject.getJSONArray(BUCKET_COLS_KEY).length(); i++) {
                    bucketColumns.add(sdJSONObject.getJSONArray(BUCKET_COLS_KEY).getString(i));
                }
            }

            Map<String, String> parameters = new HashMap<>();
            if (sdJSONObject != null && sdJSONObject.has(PARAMETERS_KEY)
                && sdJSONObject.getString(PARAMETERS_KEY) != null) {
                parameters = readMapObjectFromJson(sdJSONObject.getJSONObject(PARAMETERS_KEY).toString());
            }

            sd = new StorageDescriptor();
            sd.setCols(fieldSchemaList);
            sd.setLocation(sdJSONObject.getString(LOCATION_STR));
            sd.setInputFormat(sdJSONObject.getString(INPUT_FORMAT_STR));
            sd.setOutputFormat(sdJSONObject.getString(OUTPUT_FORMAT_STR));
            sd.setCompressed(sdJSONObject.has(COMPRESSED_STR) ? (Boolean) sdJSONObject.get(COMPRESSED_STR) : false);
            if (sdJSONObject.has(NUM_BUCKETS_KEY)) {
                sd.setNumBuckets((Integer) sdJSONObject.get(NUM_BUCKETS_KEY));
            }
            sd.setSerdeInfo(serDeInfo);
            sd.setBucketCols(bucketColumns);
            sd.setSortCols(null);
            sd.setParameters(parameters);
        }
        return sd;
    }

    private static List<FieldSchema> extractPartitionSchemaFields(JSONObject tableObject) throws JSONException {
        List<FieldSchema> partitionSchemaList = null;
        if (tableObject != null && tableObject.has(PARTITION_KEYS_STR)) {
            if (tableObject.getJSONArray(PARTITION_KEYS_STR) != null) {
                JSONArray partitionArray = tableObject.getJSONArray(PARTITION_KEYS_STR);
                partitionSchemaList = new ArrayList<>(partitionArray.length());
                for (int i = 0; i < partitionArray.length(); i++) {
                    String comment = partitionArray.getJSONObject(i).has(COMMENT_STR) ?
                        (String) partitionArray.getJSONObject(i).get(COMMENT_STR) : "";
                    FieldSchema partitionSchema = new FieldSchema(
                        partitionArray.getJSONObject(i).getString(NAME_STR),
                        partitionArray.getJSONObject(i).getString(TYPE_STR), comment);
                    partitionSchemaList.add(partitionSchema);
                }
            }
        }
        return partitionSchemaList;
    }
}
