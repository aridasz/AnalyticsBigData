package org.am.cdo.data.store;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import tech.tablesaw.api.Table;

public class FileStore implements IDataStore{

	FileWriter factorsFile = null;
	FileWriter positionsFile  = null;
	String layout = "long";
	String pattern = "yyyy-MM-dd";
	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("yyyyMM");
	DateTimeFormatter yearFormatter = DateTimeFormatter.ofPattern("yyyy");
	public FileStore(){
	}
	
	private void createFactorsFile(String suffix) {
		try{
		String fileName = "factors-"+suffix+"-"+layout+".csv";
		File file  = new File(fileName);
		System.out.println("Saving to "+file.getAbsolutePath());
		factorsFile = new FileWriter(file);
		factorsFile.write("BusinessDate,BusinessDateMonth,SecurityId,FactorName,FactorValue\n");
		}catch(Exception ex){
			
		}
	}
	
	private void createPositionsFile(String suffix) {
		try{
		String fileName = "positions-"+suffix+".csv";
		File file  = new File(fileName);
		System.out.println("Saving to "+file.getAbsolutePath());
		positionsFile = new FileWriter(file);
		positionsFile.write("BusinessDate,BusinessDateMonth,PortfolioId,SecurityId,Weight\n");
		}catch(Exception ex){
			
		}
	}
	
/*	@Override
	public void saveFactor(String securityId, LocalDate businessDate, String factorName, float factorValue) {
		try {
			factorsFile.write(businessDate.format(dateFormatter));
			factorsFile.write(","+securityId);
			factorsFile.write(","+factorName);
			factorsFile.write(","+factorValue);
			factorsFile.write("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/

	@Override
	public void saveFactor(String securityId, LocalDate businessDate, Map<String, String> factors) {
		if(factorsFile == null){
			createFactorsFile(businessDate.format(dateFormatter));
		}
		// TODO Auto-generated method stub
		try {
			if("wide".equals(layout)){
				factorsFile.write(businessDate.format(dateFormatter));
				factorsFile.write(","+businessDate.format(yearFormatter));
				factorsFile.write(","+businessDate.format(monthFormatter));
				factorsFile.write(","+securityId);
				for(String factorName : factors.keySet()){
					factorsFile.write(","+factors.get(factorName));
				}
				factorsFile.write(","+securityId);
				factorsFile.write("\n");
			}else{
				for(String factorName : factors.keySet()){
					factorsFile.write(businessDate.format(dateFormatter));
					factorsFile.write(","+businessDate.format(yearFormatter));
					factorsFile.write(","+businessDate.format(monthFormatter));
					factorsFile.write(","+securityId);
					factorsFile.write(","+factorName);
					factorsFile.write(","+factors.get(factorName));
					factorsFile.write("\n");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void savePosition(String portfolioId, LocalDate businessDate, String securityId, double weight) {
		if(positionsFile == null){
			createPositionsFile(businessDate.format(dateFormatter));
		}
		// TODO Auto-generated method stub
		try {
			positionsFile.write(businessDate.format(dateFormatter));
			positionsFile.write(","+businessDate.format(yearFormatter));
			positionsFile.write(","+businessDate.format(monthFormatter));
			positionsFile.write(","+portfolioId);
			positionsFile.write(","+securityId);
			positionsFile.write(","+weight);
			positionsFile.write("\n");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
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
		try {
			if(factorsFile != null) {
				factorsFile.flush();
			}
			if(positionsFile != null) {
				positionsFile.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setLayout(String layout) {
		this.layout = layout;
	}

	@Override
	public Table getFactors(String[] securityIds, String[] factors, String startDate, String endDate) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
