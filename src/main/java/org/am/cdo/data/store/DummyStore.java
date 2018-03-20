package org.am.cdo.data.store;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import tech.tablesaw.api.Table;

public class DummyStore implements IDataStore{

	@Override
	public void saveFactor(String securityId, LocalDate businessDate, Map<String, String> factors) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void savePosition(String portfolioId, LocalDate businessDate, String securityId, double weight) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<List<String>> getFactors(String[] securityIds, String[] factorNames, LocalDate startDate,
			LocalDate endDate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startBatch() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endBatch() {
		// TODO Auto-generated method stub
		
	}
	
	public void setLayout(String layout) {
		
	}

	@Override
	public Table getFactors(String[] securityIds, String[] factors, String startDate, String endDate) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void lookupPortfolio(String portfolioId, String startDate, String endDate) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void saveFactors(int batchSize, Table dataTab) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void connect() {
		// TODO Auto-generated method stub
		
	}


}
