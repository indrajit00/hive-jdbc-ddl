package com.zaloni.hiveApi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveConnectionProvider {

	private static final String driver = "org.apache.hive.jdbc.HiveDriver";
	private static Statement state;
	private static Connection connect;

	public static void establishConnection() throws SQLException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			System.out.println("Error : Driver Not Found");
			e.printStackTrace();
			System.exit(1);
		}
		connect = DriverManager.getConnection("jdbc:hive2://192.168.1.135:10000/", "zaloni", "zaloni");
		System.out.println("!!!Connection established successfully!!!");
		state = connect.createStatement();
	}

	public static Statement getStatement() throws SQLException {
		if (state != null) {
			return state;
		}
		if (connect == null) {
			establishConnection();
		}
		state = connect.createStatement();
		return state;

	}
	public static  void closeConnection() throws SQLException
	{
		connect.close();
	}

}
