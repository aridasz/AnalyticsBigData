package org.am.cdo;

import java.util.HashMap;

import org.am.cdo.data.store.CassandraStoreAWS;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import tech.tablesaw.api.Table;

@SpringBootApplication
public class AnalyticsApp implements CommandLineRunner {
	
	@Autowired
	CassandraStoreAWS cassandraStore;

	public static void main(String[] args) {
		SpringApplication.run(AnalyticsApp.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		HashMap<String,String> parameters = new HashMap<String,String>();
		for(int i=0;i<args.length;i=i+2){
			parameters.put(args[i], args[i+1]);
		}
		System.out.println(parameters);
        String portfolioId = parameters.get("-portfolioId");
		String[] secIds = parameters.get("-securities") != null ? parameters.get("-securities").split(",") : null;
		String[] factors = parameters.get("-factors") != null ? parameters.get("-factors").split(",") : null;
		String startDate = parameters.get("-start");
		String endDate = parameters.get("-end");
		int batchSize = parameters.get("-batchsize") != null ? Integer.parseInt(parameters.get("-batchsize")) : 500;
		              
		cassandraStore.connect();		
		
		try {
						
			if(secIds != null && secIds.length > 0 && factors != null && factors.length > 0) {
				System.out.println("Getting Factors Calc and Calc daily return....");
				Table dataTab = cassandraStore.getFactors(secIds, factors, startDate, endDate);
				cassandraStore.saveFactors(batchSize, dataTab);
			}
			
			//"SP500", "1999-01-01", "2018-01-01"
			if(StringUtils.isNotBlank(portfolioId)) {
				System.out.println("Getting portfolio daily return for : " + portfolioId);
				cassandraStore.lookupPortfolio(portfolioId, startDate, endDate);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cassandraStore.close();
		}
	}
}
