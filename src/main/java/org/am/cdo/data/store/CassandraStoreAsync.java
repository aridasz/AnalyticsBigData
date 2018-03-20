package org.am.cdo.data.store;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.am.cdo.util.AnalyticsUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import tech.tablesaw.api.CategoryColumn;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

@Component("cassandraStoreAsync")
public class CassandraStoreAsync implements IDataStore {
	
	@Value("${cassandra.conn.url}")
	String connectionPoints;
	
	@Value("${cassandra.conn.user}")
	String username;
	
	@Value("${cassandra.conn.pass}")
	String password;
	
	@Value("${cassandra.cluster.dc}")
	String clusterDC;
	
	@Value("${cassandra.conn.port}")
	Integer port;
	
	@Value("${cassandra.conn.cloud.dc}")
	boolean isConnectCloudDC;

	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	private static List<ResultSetFuture> sendQueries(Session session, String query, Object[] partitionKeys) {
	    List<ResultSetFuture> futures = Lists.newArrayList();
	    System.out.println("partition size: " + partitionKeys.length);
	    for (Object partitionKey : partitionKeys)
	        futures.add(session.executeAsync(String.format(query, partitionKey)));
	    return futures;
	}
	
	private static Table extractResults(List<ResultSetFuture> futures, String factors[]) {
		Column[] columns = new Column[factors.length + 2];
		columns[0] = new CategoryColumn("security_id");
		columns[1] = new DateColumn("business_date");

		for (int i = 0; i < factors.length; i++) {
			columns[i + 2] = new FloatColumn(factors[i]);
		}

		Table table1 = Table.create("security_factors", columns);

		for (ResultSetFuture future : futures) {
			ResultSet rs = future.getUninterruptibly();
			try {
				AnalyticsUtil.readResultSetToTable(table1, rs);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		table1.sortAscendingOn("security_id", "business_date");

		AnalyticsUtil.combinedFactCalcDaily(table1, factors);
		AnalyticsUtil.combinedReturnDaily(table1, factors);

		return table1;
	}
	
	public Table getFactors(String[] securityIds, String factors[], 
			String startDate, String endDate) throws Exception {
		
		String factorNames = Arrays.stream(factors).collect(Collectors.joining(","));
		long t1 = System.currentTimeMillis();
		
		String query = "select security_id, business_date, " + factorNames + " from amcdopoc.security_factors "
				+ " where security_id = '%s' "
				+ " and business_date > '" + startDate + "' and business_date < '" + endDate + "'";

		
		Table table = extractResults(sendQueries(session, query, securityIds), factors);
		
		long t2 = System.currentTimeMillis();
		System.out.println("--- all done ---- count: " + table.rowCount() + " in sec: " + (t2-t1)/1000.0);
		System.out.println(table.selectWhere(table.categoryColumn("security_id").isEqualTo("MSFT")).print());
		
		return table;
	}

	public void saveFactors(int batchSize, Table dataTab) throws Exception {
		
		ExecutorService executorService = Executors.newFixedThreadPool(4);
		CompletionService<Integer> executorCompletionService= new ExecutorCompletionService<Integer>(executorService );
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		long t1 = System.currentTimeMillis();
		
		List<Table> partitions = AnalyticsUtil.splitForBatch(batchSize, dataTab);
		
		final String update = "update amcdopoc.security_factors set calc1 = %s, calc_daily_return1 = %s where security_id = '%s' and business_date = '%s'";
		
		for (Table tab : partitions) {
			final Table partition = tab;
			futures.add(executorCompletionService.submit(new Callable<Integer>() {

				@Override
				public Integer call() throws Exception {		
					
					int size = partition.rowCount();
					CategoryColumn securityId = partition.categoryColumn("security_id");
					DateColumn date = partition.dateColumn("business_date");
					FloatColumn calcReturn = partition.floatColumn("combined_calc_daily");
					FloatColumn calcDailyReturn = partition.floatColumn("daily_return");
					
					Batch batch = QueryBuilder.unloggedBatch();
					int count = 0;
					for(int i = 0; i < size; i++) {
						batch.add(new SimpleStatement(String.format(update, calcReturn.getFloat(i), calcDailyReturn.getFloat(i),
								securityId.getString(i), date.getString(i))));
						count++;
					}
					
					session.execute(batch);					
					return count;
				}
			}));
		}
		
		int count = 0;
		for (Future<Integer> fut : futures) {
			count = count + fut.get();
		}	
		
		long t2 = System.currentTimeMillis();
		System.out.println("### saved, count: in sec: " + count + " | " + (t2-t1)/1000.0);
		executorService.shutdown();

	}
	
		
	public Map<String, Double> getAggrFactors(Set<String> securityIdSet, String startDate, String endDate, 
			Map<com.datastax.driver.core.LocalDate, Map<String, Double>> securityWeightByDate) throws Exception {
		
		String securities =  "'" + Joiner.on("','").join(securityIdSet) + "'";
		//System.out.println(securities);
		
		String query = "select security_id, business_date, calc_daily_return1 from amcdopoc.security_factors "
				+ " where security_id in (" + securities + ") "
				+ " and business_date >= '" + startDate + "'" + " and business_date <= '" + endDate + "'";
		
		long t1 = System.currentTimeMillis();
		final ResultSet rs = getSession().execute(query);
		long t2 = System.currentTimeMillis();
		//System.out.println("Time taken query part 1: in sec: " + (t2-t1)/1000.0);
		
		Map<String, Double> portfolioDayRet = Maps.newLinkedHashMap();
		
		com.datastax.driver.core.LocalDate dateInPrevItr = null;
		Double portfolioReturn = null;
		int count2 = 0;
		Map<String, Double> lookupMapForDate = null;
		
		for(Row row : rs) {
			com.datastax.driver.core.LocalDate businessDate = row.getDate("business_date");
			String securityId = row.getString("security_id");
			
			if(!businessDate.equals(dateInPrevItr)) {
				lookupMapForDate = securityWeightByDate.get(businessDate);
				portfolioReturn = 0.0;
			} 			
			
			if(lookupMapForDate.containsKey(securityId)) {
				portfolioReturn = portfolioReturn + (row.getDouble("calc_daily_return1") * lookupMapForDate.get(securityId));
			}
			dateInPrevItr = businessDate;
			portfolioDayRet.put(businessDate.toString(), portfolioReturn);
			
			count2++;
		}
		
		long t3 = System.currentTimeMillis();
		System.out.println(String.format("Time taken part 2: count: %s, in sec: %s ", count2, (t3-t2)/1000.0));
		return portfolioDayRet;
	}

	
    public void lookupPortfolio(String portfolioId, String startDt, String endDt) throws Exception {
		
	    final String qry = "select business_date, security_id, weight from amcdopoc.portfolio_positions where "
	    		+ "portfolio_id = '%s' and business_date >= '%s' and business_date <= '%s'";
	    
	    long t1 = System.currentTimeMillis();
	    final ResultSet rs = getSession().execute(String.format(qry, portfolioId, startDt, endDt));
	    long t2 = System.currentTimeMillis();
	    //System.out.println(String.format("Time taken query part 1: in sec: %s", (t2-t1)/1000.0));
	    
	    Set<String> securitiesSet = Sets.newHashSet();
	    Map<com.datastax.driver.core.LocalDate, Map<String, Double>> securityWeightByDate = Maps.newHashMap();
	    
	    int count1 = 0;
	    for (Row row : rs) {
	    	com.datastax.driver.core.LocalDate businessDate = row.getDate("business_date");
	    	if(!securityWeightByDate.containsKey(businessDate)) {
	    		securityWeightByDate.put(businessDate, Maps.newHashMap());
	    	} 
	    	String securityId = row.getString("security_id");
	    	securityWeightByDate.get(businessDate).put(securityId, row.getDouble("weight"));
	    	securitiesSet.add(securityId);
	    	count1++;
		}
	    long t3 = System.currentTimeMillis();
	    System.out.println(String.format("Time taken part 1 processing: count: %s, in sec: %s", count1, (t3-t2)/1000.0));
	    
	    Map<String, Double> portfolioReturns = getAggrFactors(securitiesSet, startDt, endDt, securityWeightByDate);
	    
	    System.out.println("Done.... date points: " + portfolioReturns.size());
        
        int top = 0;
        for (Entry<String, Double> entry : portfolioReturns.entrySet()) {
			System.out.println(entry.getKey() + "   |   " + entry.getValue());
            top++;
            if(top == 10) {
                System.out.println("..........................");
                break;
            }
		}
	}
	
	private Session session;
	private Cluster cluster;
	
	private Session getSession() {
		if(session == null) {
			connect();
		}
		return session;
	}
	
	public void connect() {
		PoolingOptions poolOpt = new PoolingOptions();
		poolOpt.setMaxQueueSize(10000);
		
		
		Cluster.Builder clusterBuilder = Cluster.builder().addContactPoints(connectionPoints.split(",")).withPort(port)
				.withPoolingOptions(poolOpt);
		
		if(isConnectCloudDC) {
			clusterBuilder.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("AWS_VPC_US_EAST_1").build());
			clusterBuilder.withAuthProvider(new PlainTextAuthProvider(username, password));
		}
		
		/*Cluster.Builder clusterBuilder = Cluster.builder()
				.addContactPoints(
						"23.20.121.214", "34.195.86.103", "35.172.77.138" // AWS_VPC_US_EAST_1 (Amazon WebServices (VPC))
				).withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder()
						.withLocalDc("AWS_VPC_US_EAST_1").build()) // your // local data centre
				.withPort(9042)
				.withAuthProvider(new PlainTextAuthProvider("iccassandra", "a832a593f4bd5098d30e77776c668c77"));*/
		
		this.cluster = clusterBuilder.build();
		final Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		session = cluster.connect();
	}

	public void connectLocal() {
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		final Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}
	
	/** Close cluster. */
	public void close() {
		cluster.close();
	}

	/**
	 * arg0 - portfolioId
	 * arg1 - comma separated list of securities
	 * arg2 - comma separated list of factor names
	 * arg3 - startDate (yyyy-mm-dd)
	 * arg4 - endDate (yyyy-mm-dd)
	 * arg5 - batchSize
	 * -portfolioId SP500 -securities GOOG,AMZN,MSFT -factors factor1,factor2,factor3 -start 1999-01-01 -end 2018-01-01 -batchsize 800
	 * @param args
	 */
	public static void main(String[] args) {
		CassandraStoreAsync client = new CassandraStoreAsync();
	
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
		              
		client.connect();		
		
		try {
						
			if(secIds != null && secIds.length > 0 && factors != null && factors.length > 0) {
				System.out.println("Getting Factors Calc and Calc daily return....");
				Table dataTab = client.getFactors(secIds, factors, startDate, endDate);
				client.saveFactors(batchSize, dataTab);
			}
			
			//"SP500", "1999-01-01", "2018-01-01"
			if(StringUtils.isNotBlank(portfolioId)) {
				System.out.println("Getting portfolio daily return for : " + portfolioId);
				client.lookupPortfolio(portfolioId, startDate, endDate);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
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

	@Override
	public void setLayout(String layout) {
		// TODO Auto-generated method stub
		
	}

}
