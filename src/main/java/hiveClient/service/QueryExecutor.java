package hiveClient.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import hiveClient.dao.Result;
import hiveClient.pojo.QueryEntity;
import hiveClient.repository.ResultRepository;

@Service
public class QueryExecutor {

	@Autowired
	ResultRepository repository;

	public List<Result> getResults(QueryEntity qe) {
		String query = qe.getQuery();
		List<Result> resultList = null;
		if (qe.getParams() == null || qe.getParams().isEmpty()) {
			resultList = repository.findResult(query);
		} else {
			resultList = repository.findResult(query, qe.getParams());
		}
		return resultList;
	}
}
