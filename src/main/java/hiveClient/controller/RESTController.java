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
	private void connect() {
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

	private String hiveSettingsSetter(List<String> list) {
		Boolean result = null;
		try {
			Statement createStatement = con.createStatement();
			for(String setting: list){
				result = createStatement.execute(setting);
			}
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
	public @ResponseBody List<Map<String, String>> queryExecutor(@RequestBody QueryEntity queryEntity) {
		connect();
		List<String> settingList = new ArrayList<>();
		settingList.add("SET mapreduce.job.queuename=dashboard");
		settingList.add("SET hive.exec.parallel=true");
		settingList.add("SET hive.vectorized.execution.enabled=true");
		settingList.add("SET hive.optimize.bucketmapjoin=true");
		hiveSettingsSetter(settingList);
		logger.debug("The query is : " + queryEntity.getQuery());
		
		List<Map<String, String>> arrayList = new ArrayList<Map<String, String>>();
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
			System.out.println("The number of columns is " + numberOfColumns);
			for (int i = 1; i <= numberOfColumns; i++) {
				String colName = metadata.getColumnLabel(i);
				columns.add(colName);
			}

			while (res.next()) {
				int i = 1;
				Map<String, String> result = new HashMap<String, String>();
				while (i <= numberOfColumns) {
					String colVal = res.getString(i);
					result.put(columns.get(i-1), colVal);
					i++;
				}
				arrayList.add(result);
			}
		} catch (SQLException e) {
			logger.error("SQL exception for " + e.getMessage());
			e.printStackTrace();
		}finally {
			close();
		}

		return arrayList;
	}
	
	
	/**
	 * Function to close the connection.
	 */
	private void close() {
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
