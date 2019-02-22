package com.zaloni.hiveApi;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveQueryExecutor {

	public static boolean execute(String query) throws SQLException {
		System.out.println("Executing query -> " + query);
		return HiveConnectionProvider.getStatement().execute(query);
	}

	public static ResultSet executeQuery(String query) throws SQLException {
		System.out.println("Executing query -> " + query);
		return HiveConnectionProvider.getStatement().executeQuery(query);
	}

}
