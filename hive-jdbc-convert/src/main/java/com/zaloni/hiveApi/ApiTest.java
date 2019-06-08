package com.zaloni.hiveApi;

import java.sql.SQLException;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.zaloni.hiveApi.HiveTableDesc;
import com.fasterxml.jackson.core.JsonParseException;
import java.io.*;

public class ApiTest {

	public static void main(String[] args) throws SQLException, org.json.simple.parser.ParseException, JsonParseException, JsonMappingException, IOException {
		
		HiveTableDesc hiveTableRequest = new ObjectMapper().readValue(new File("C:\\Users\\rmour\\Desktop\\hack\\jsontest.json"), HiveTableDesc.class);
		MetaDataOperationsExecutor.executeTask("DELETE",hiveTableRequest);
	}
}