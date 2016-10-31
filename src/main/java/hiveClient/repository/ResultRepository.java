package hiveClient.repository;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import hiveClient.dao.Result;

@Repository
public class ResultRepository {
	
	@Autowired
	private NamedParameterJdbcTemplate  jdbcTemplate;
	
	public List<Result> findResult(String queryString){
		return jdbcTemplate.query(queryString, new ResultMapper());
	}
	
	public List<Result> findResult(String queryString, Map<String, String> params){
		return jdbcTemplate.query(queryString, params, new ResultMapper());
	}
}
