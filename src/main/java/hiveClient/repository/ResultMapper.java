package hiveClient.repository;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

import hiveClient.dao.Result;

class ResultMapper implements RowMapper<Result>{
	@Override
	public Result mapRow(ResultSet res, int rowNum) throws SQLException {
		ResultSetMetaData metadata = res.getMetaData();
		int numberOfColumns = metadata.getColumnCount();
		List<String> columns = new ArrayList<String>();
		for (int i = 1; i <= numberOfColumns; i++) {
			String colName = metadata.getColumnLabel(i);
			columns.add(colName);
		}
		Result resObj = new Result();
		int i = 1;
		Map<String, String> result = new HashMap<String, String>();
		while (i <= numberOfColumns) {
			String colVal = res.getString(i);
			result.put(columns.get(i - 1), colVal);
			i++;
		}
		resObj.setResultMap(result);
		return resObj;
	}
}
