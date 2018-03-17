package org.am.cdo.data.store;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import tech.tablesaw.api.Table;

public interface IDataStore {
	public void saveFactor(String securityId, LocalDate businessDate, Map<String,String> factors);
	public void savePosition(String portfolioId, LocalDate businessDate, String securityId, double weight);
	//public void saveFactor(String securityId, LocalDate businessDate, String factorName, float factorValue);
	//public void savePosition(int portfolioId, LocalDate businessDate, int securityId, Map<String,String> attributes);
	
	public List<List<String>> getFactors(String[] securityIds, String[] factorNames, LocalDate startDate, LocalDate endDate);
	
	public void startBatch();
	public void endBatch(); 
	
	public void setLayout(String layout);
	public Table getFactors(String[] securityIds, String[] factors, String startDate, String endDate) throws Exception;
}
