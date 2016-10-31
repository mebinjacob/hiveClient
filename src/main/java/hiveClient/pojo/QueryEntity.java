package hiveClient.pojo;

import java.util.Map;

public class QueryEntity {

	private String query;
	
	private Map<String, String> params;

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public Map<String, String> getParams() {
		return params;
	}

}
