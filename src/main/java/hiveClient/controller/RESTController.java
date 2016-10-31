package hiveClient.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import hiveClient.dao.Result;
import hiveClient.pojo.QueryEntity;
import hiveClient.service.QueryExecutor;

@RestController
@RequestMapping("/api/v1")
public class RESTController {
	
	@Autowired
	QueryExecutor queryExecutor;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass()); 

	@RequestMapping(value = "/query", method = RequestMethod.POST)
	public @ResponseBody List<Map<String, String>> queryExecutor(@RequestBody QueryEntity queryEntity) {
		
		logger.info(" A query has been received " );
	    String queryInfo = queryEntity.getQuery();
		logger.info("The query is " + queryInfo);
		List<Result> results = queryExecutor.getResults(queryEntity);
		
		List<Map<String, String>> returnFormatList = new ArrayList<>();
		for(Result result : results){
			returnFormatList.add(result.getResultMap());
		}
		
		logger.info("Finished procecssing !! ");
		return returnFormatList;
	}
	
}
