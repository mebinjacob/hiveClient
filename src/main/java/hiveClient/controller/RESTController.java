package hiveClient.controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.testng.log4testng.Logger;

import hiveClient.pojo.QueryEntity;

@RestController
@RequestMapping("/api/v1")
public class RESTController {

	@Value("${driverName}")
	String driverName;

	@Value("${jdbcURL}")
	String jdbcURL;

	@Value("${username}")
	String username;

	@Value("${password}")
	String password;

	private final Logger logger = Logger.getLogger(this.getClass());

	@RequestMapping("/")
	public String viewProductList() {
		return "admin";
	}

	Connection con = null;

	/**
	 * Function to establish the connection, the same connection is then used
	 * across.
	 */
	@RequestMapping(value = "/connect")
	public void connect() {
		try {
			Class.forName(driverName);
			con = DriverManager.getConnection(jdbcURL, username, password);
		} catch (SQLException e) {
			logger.error("Sql exception is " + e.getMessage());
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			logger.error("Class not found exception for " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

	@RequestMapping(value = "/customQuery", method = RequestMethod.POST)
	public @ResponseBody String submitSQLREST(@RequestBody Map<String, String> map) {
		Boolean result = null;
		try {
			Statement createStatement = con.createStatement();
			result = createStatement.execute(map.get("query"));
		} catch (SQLException e) {
			logger.error("SQL exception for " + e.getMessage());
			e.printStackTrace();
		}
		
		if(result != null && result == false){
			return "Executed and there is nothing to return";
		}
		return result.toString();
	}	
	
	@RequestMapping(value = "/query", method = RequestMethod.POST)
	public @ResponseBody Map<String, String> submitSQLREST(@RequestBody QueryEntity queryEntity) {

		logger.debug("The query is : " + queryEntity.getQuery());
		Map<String, String> result = new HashMap<String, String>();
		List<String> arrayList = new ArrayList<String>();
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			logger.error("Class not found exception for " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
		try {

			Statement stmt = con.createStatement();
			PreparedStatement prepStmt = con.prepareStatement(queryEntity.getQuery());
			prepStmt.setString(1, queryEntity.getGfeVersions());
			prepStmt.setString(2, queryEntity.getStartDate());
			prepStmt.setString(3, queryEntity.getEndDate());
			prepStmt.setString(4, queryEntity.getStartDate());
			prepStmt.setString(5, queryEntity.getEndDate());
			ResultSet res = prepStmt.executeQuery();
			ResultSetMetaData metadata = res.getMetaData();
			int numberOfColumns = metadata.getColumnCount();
			List<String> columns = new ArrayList<String>();

			for (int i = 1; i <= numberOfColumns; i++) {
				String colName = metadata.getColumnLabel(i);
				columns.add(colName);
			}

			while (res.next()) {
				int i = 1;
				while (i <= numberOfColumns) {
					String colVal = res.getString(i++);
					result.put(columns.get(i), colVal);
					arrayList.add(colVal);
				}
			}
		} catch (SQLException e) {
			logger.error("SQL exception for " + e.getMessage());
			e.printStackTrace();
		}

		return result;
	}
	
	
	/**
	 * Function to close the connection.
	 */
	@RequestMapping(value = "/close")
	public void close() {
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}

// @RequestMapping(value = "/submit", method = RequestMethod.POST)
// public String submitSQL(String val) {
//
// try {
// Class.forName(driverName);
// } catch (ClassNotFoundException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// System.exit(1);
// }
// try {
// Connection con =
// DriverManager.getConnection("jdbc:hive2://hadoop-hive-dc1-1:9443/default", ,
// );
// Statement stmt = con.createStatement();
// ResultSet res = stmt.executeQuery(val);
// ResultSetMetaData metadata = res.getMetaData();
// int numberOfColumns = metadata.getColumnCount();
// List<String> columns = new ArrayList<String>();
// for (int i = 1; i <= numberOfColumns; i++) {
//
// String colName = metadata.getColumnLabel(i);
// System.out.println("The col name is " + colName);
// columns.add(colName);
// }
//
// List<String> arrayList = new ArrayList<String>();
// while (res.next()) {
// int i = 1;
// while (i <= numberOfColumns) {
// String colVal = res.getString(i++);
// System.out.println("Col value is " + colVal);
// arrayList.add(colVal);
// }
// }
// } catch (SQLException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
//
// System.out.println("The value is " + val);
// return "admin";
// }
