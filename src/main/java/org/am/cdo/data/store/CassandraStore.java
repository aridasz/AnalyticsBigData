package org.am.cdo.data.store;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.am.cdo.util.Utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

import tech.tablesaw.api.Table;

public class CassandraStore implements IDataStore{

	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
/*	use vg_test;

	create table security_factor
	(
	    SecurityId int, 
	    BusinessDate date,
	    Month int,
	    FactorName text,
	    FactorValue double
	    PRIMARY KEY ((SecurityId, Month), BusinessDate)
	) WITH CLUSTERING ORDER BY (date DESC);*/

	
	public void saveFactor(String securityId, LocalDate businessDate, String factorName, float factorValue) {
		// TODO Auto-generated method stub
		
	}

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
		String factors = Arrays.stream(factorNames).collect(Collectors.joining(","));
		String securities = "'"+Arrays.stream(securityIds).collect(Collectors.joining("','"))+"'";
		String query = "SELECT Security_id, Business_Date,"+factors+"  from amcdopoc.security_factors" + 
				" WHERE Security_Id in ("+securities+")" + 
				" AND Business_Date >= '" +startDate.format(dateFormatter)+"'"+ 
				" AND Business_Date <= '"+endDate.format(dateFormatter)+"'";
		System.out.println(query);
		long startTime = System.currentTimeMillis();
		getSession();
		ResultSet rs = session.execute(query);
		long endTime = System.currentTimeMillis();
		System.out.println("Query Exec timetaken="+Utils.formatTime(endTime-startTime));
		startTime = System.currentTimeMillis();
		int rowCount = 0;
		for(Row row : rs) {
			row.getString("Security_id");
			rowCount++;
		}
		endTime = System.currentTimeMillis();
		System.out.println("Query Fetch noRows="+rowCount+" timetaken="+Utils.formatTime(endTime-startTime));
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
	
	Session session;
	private Session getSession() {
		if(session == null) {
			Cluster.Builder clusterBuilder = Cluster.builder()
			    .addContactPoints(
			        "23.20.121.214", "34.195.86.103", "35.172.77.138" // AWS_VPC_US_EAST_1 (Amazon Web Services (VPC))
			    )
			    .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("AWS_VPC_US_EAST_1").build()) // your local data centre
			    .withPort(9042)
			    .withAuthProvider(new PlainTextAuthProvider("iccassandra", "a832a593f4bd5098d30e77776c668c77"));
			Cluster cluster = clusterBuilder.build();
			session = cluster.connect();
		}
		return session;
	}
	
	public void setLayout(String layout) {
		
	}

	@Override
	public Table getFactors(String[] securityIds, String[] factors, String startDate, String endDate) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


}
