package hiveClient.pojo;

public class QueryEntity {
	private String query;
	private String startDate;
	private String endDate;
	private String gfeVersions; 
	public String getGfeVersions() {
		return gfeVersions;
	}
	public void setGfeVersions(String gfeVersions) {
		this.gfeVersions = gfeVersions;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	
}
