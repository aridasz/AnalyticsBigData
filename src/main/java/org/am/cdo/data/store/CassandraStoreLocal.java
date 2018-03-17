package org.am.cdo.data.store;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.am.cdo.util.AnalyticsUtil;
import org.apache.commons.lang3.ArrayUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
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

public class CassandraStoreLocal implements IDataStore {

	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	@Override
	public Table getFactors(String[] securityIds, String factors[], String startDate, String endDate) throws Exception {
		
		String factorNames = Arrays.stream(factors).collect(Collectors.joining(","));
		
		List<String> securityIdList = Arrays.asList(securityIds);
		List<List<String>> securityPartitions = Lists.partition(securityIdList, 100);
		System.out.println(securityPartitions.size());
		
		ExecutorService executorService = Executors.newFixedThreadPool(4);
		CompletionService<Table> executorCompletionService= new ExecutorCompletionService<Table>(executorService);
		List<Future<Table>> futures = new ArrayList<Future<Table>>();
		
		long t1 = System.currentTimeMillis();
		
		int loop = 0;
		for (List<String> parts : securityPartitions) {
			final int index = loop;
			loop++;
			futures.add(executorCompletionService.submit(new Callable<Table>() {

				@Override
				public Table call() throws Exception {				
					String securities =  "'" + Joiner.on("','").join(parts) + "'";
					
					String query = "select security_id, business_date, " + factorNames + " from analyticspoc.security_factors_2 "
							+ " where security_id in (" + securities + ") "
							+ " and business_date > '" + startDate + "' and business_date < '" + endDate + "'";
					
					ResultSet rs = getSession().execute(query);
					Table table1 = Table.create("security_factors"+index, new CategoryColumn("security_id"), new DateColumn("business_date"), 
							new FloatColumn("open"), new FloatColumn("close"), 
							new FloatColumn("high"), new FloatColumn("low"));

					try { 
						AnalyticsUtil.readResultSetToTable(table1, rs);
						
						table1.sortAscendingOn("security_id", "business_date");
						
						AnalyticsUtil.combinedFactCalcDaily(table1, factors);
						AnalyticsUtil.combinedReturnDaily(table1, factors);
									
					} catch (Exception e) {
						e.printStackTrace();
					}
					return table1;
				}
			}));
		}
		
		Table table = null; 
		for (Future<Table> fut : futures) {
			if(table == null) {
				table = fut.get();
			} else {
				table.append(fut.get());
			}
		}	
		
		long t2 = System.currentTimeMillis();
		System.out.println("--- all done ---- count: " + table.rowCount() + " in sec: " + (t2-t1)/1000.0);
		//System.out.println(table.print(100));
		System.out.println(table.selectWhere(table.categoryColumn("security_id").isEqualTo("MSFT")).print());
		
		executorService.shutdown();
		
		/*String securities = "'"+Arrays.stream(securityIds).collect(Collectors.joining("','"))+"'";
		
		String query = "select security_id, business_date, " + factorNames + " from analyticspoc.security_factors_2 "
				+ " where security_id in (" + securities + ") "
				+ " and business_date > '" + startDate + "' and business_date < '" + endDate + "'";
		
		System.out.println(query);
		
		long t1 = System.currentTimeMillis();
		final ResultSet rs = getSession().execute(query);
		long t2 = System.currentTimeMillis();
		System.out.println("--- got results---- in sec: " + (t2-t1)/1000.0);*/

		/*Table table1 = Table.create("security_factors", new CategoryColumn("security_id"), new DateColumn("business_date"), 
				new FloatColumn("open"), new FloatColumn("close"), 
				new FloatColumn("high"), new FloatColumn("low"));

		try { 
			AnalyticsUtil.readResultSetToTable(table1, fullResults);
			
			table1.sortAscendingOn("security_id", "business_date");
			
			AnalyticsUtil.combinedFactCalcDaily(table1, factors);
			AnalyticsUtil.combinedReturnDaily(table1, factors);
						
			long t3 = System.currentTimeMillis();
			System.out.println("done, count | in sec: " + table1.rowCount() + " | " + (t3-t2)/1000.0);
			
			//System.out.println(table1.structure().print());			
			System.out.println(table1.print(100));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
*/		
		return table;
	}

	public void saveFactors(Table dataTab) throws Exception {
		
		ExecutorService executorService = Executors.newFixedThreadPool(4);
		CompletionService<Integer> executorCompletionService= new ExecutorCompletionService<Integer>(executorService );
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		long t1 = System.currentTimeMillis();
		
		int batchSize = 700;
		List<Table> partitions = AnalyticsUtil.splitForBatch(batchSize, dataTab);
		
		final String update = "update analyticspoc.security_factors_2 set calc_return = %s, calc_daily_return = %s where security_id = '%s' and business_date = '%s'";
		
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
	
		
	public List<Map<String, Object>> getFactorsSimple(String[] securityIds, String factors[], double[] weightage, String startDate, String endDate) throws Exception {
		
		String factorNames = Arrays.stream(factors).collect(Collectors.joining(","));
		//String securities = "'"+Arrays.stream(securityIds).collect(Collectors.joining("','"))+"'";
		
		List<String> securityIdList = Arrays.asList(securityIds);
		List<List<String>> securityPartitions = Lists.partition(securityIdList, 100);
		System.out.println(securityPartitions.size());
		
		ExecutorService executorService = Executors.newFixedThreadPool(4);
		CompletionService<List<Map<String, Object>>> executorCompletionService= new ExecutorCompletionService<List<Map<String, Object>>>(executorService);
		List<Future<List<Map<String, Object>>>> futures = new ArrayList<Future<List<Map<String, Object>>>>();
		
		long t1 = System.currentTimeMillis();
		
		for (List<String> parts : securityPartitions) {
			futures.add(executorCompletionService.submit(new Callable<List<Map<String, Object>>>() {

				@Override
				public List<Map<String, Object>> call() throws Exception {				
					String securities =  "'" + Joiner.on("','").join(parts) + "'";
					
					String query = "select security_id, business_date, " + factorNames + " from analyticspoc.security_factors_4 "
							+ " where security_id in (" + securities + ") "
							+ " and security_id_sort in (" + securities + ") "
							+ " and business_date >= '" + startDate + "' and business_date <= '" + endDate + "'";
					
					final ResultSet rs = getSession().execute(query);
					
					//Double weightage = 1.0 / factors.length;
					List<Map<String, Object>> calcRet = Lists.newArrayList();
					
					int index = 0;
					Double lastDayCalc = null;
					String lastSecId = null;
					for(Row row : rs) {
						
						String secId = row.getString("security_id");
						Map<String, Object> rowObj = Maps.newHashMap();			
						Map<String, Object> prevDayRow = secId.equals(lastSecId) ? calcRet.get(index-1) : null;
						lastSecId = row.getString("security_id");

						calcRet.add(rowObj);
						
						rowObj.put("security_id", secId);
						rowObj.put("business_date", row.getDate("business_date"));
						
						Double calc = 0.0;
						Double factValue = 0.0;
						int index_fact = 0;
						for (String factor : factors) {
							factValue = row.getDouble(factor);
							rowObj.put(factor, factValue);
							calc = calc + (factValue * weightage[index_fact]);
							index_fact++;
						}
						
						rowObj.put("combined_calc_daily", calc);	
						
						if(prevDayRow != null) {
							lastDayCalc = (Double)prevDayRow.get("combined_calc_daily");
							rowObj.put("calc_daily_return", (calc - lastDayCalc) / lastDayCalc);
						} else {
							rowObj.put("calc_daily_return", 0.0);
						}
						index++;
					}
					return calcRet;
				}
			}));
		}
		
		List<Map<String, Object>> table = null; 
		for (Future<List<Map<String, Object>>> fut : futures) {
			if(table == null) {
				table = fut.get();
			} else {
				table.addAll(fut.get());
			}
		}	
		
		long t2 = System.currentTimeMillis();
		System.out.println("--- all done ---- count: " + table.size() + " in sec: " + (t2-t1)/1000.0);
		
		executorService.shutdown();
		
		/*try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("C:\\\\projects\\\\WIKI_PRICES_Sec\\\\returns.csv")))
		{
			writer.write(table.stream().map(Object::toString).collect(Collectors.joining(",\n")).toString());
		}*/
		//System.out.println(table.stream().map(Object::toString).collect(Collectors.joining(",\n")).toString());
		
		return table;
	}

	public void saveFactorSimple(List<Map<String, Object>> dataTab) throws Exception {
		
		ExecutorService executorService = Executors.newFixedThreadPool(4);
		CompletionService<Integer> executorCompletionService= new ExecutorCompletionService<Integer>(executorService );
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		long t1 = System.currentTimeMillis();
		
		List<List<Map<String, Object>>> partitions = Lists.partition(dataTab, 500);
		System.out.println(String.format("Number of batches: %s , with batchSize: %s ", partitions.size(), "500"));
		
		final String update = "update analyticspoc.security_factors_4 set calc = %s, calc_daily_return = %s where security_id = '%s' and security_id_sort = '%s' and business_date = '%s'";
		
		for (List<Map<String, Object>> tab : partitions) {
			final List<Map<String, Object>> partition = tab;
			futures.add(executorCompletionService.submit(new Callable<Integer>() {

				@Override
				public Integer call() throws Exception {		
					
					Batch batch = QueryBuilder.unloggedBatch();
					int count = 0;
					for(int i = 0; i < partition.size(); i++) {
						batch.add(new SimpleStatement(String.format(update, 
								(Double)partition.get(i).get("combined_calc_daily"), 
								(Double)partition.get(i).get("daily_return"),
								(String)partition.get(i).get("security_id"), 
								(String)partition.get(i).get("security_id"),
								String.valueOf(partition.get(i).get("business_date")))));
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

	public Map<String, Object> getAggrFactors(String[] securityIds, double[] securityWeights, String date) throws Exception {
		
		String securities =  "'" + Joiner.on("','").join(securityIds) + "'";
		System.out.println(securities);
		
		String query = "select security_id, business_date, calc_daily_return from analyticspoc.security_factors_2 "
				+ " where security_id in (" + securities + ") "
				//+ " and security_id_sort in (" + securities + ") "
				+ " and business_date >= '" + date + "'" + " and business_date <= '" + date + "'";
		
		long t1 = System.currentTimeMillis();
		final ResultSet rs = getSession().execute(query);
		long t2 = System.currentTimeMillis();
		System.out.println("--- all done ---count:  in sec: " + (t2-t1)/1000.0);
		Map<String, Object> portfolioDayRet = Maps.newHashMap();
		
		Double portfolioReturn = 0.0;
		int index = 0;
		for(Row row : rs) {
			portfolioReturn = portfolioReturn + (row.getDouble("calc_daily_return") * securityWeights[index]);
			index++;
		}
		portfolioDayRet.put("business_date", date);
		portfolioDayRet.put("daily_return", portfolioReturn);

		
		/*long t2 = System.currentTimeMillis();
		System.out.println("--- all done ---count: " + index + " data: " + portfolioDayRet + " in sec: " + (t2-t1)/1000.0);*/
		
		return portfolioDayRet;
	}

	public Map<String, Double> getAggrFactors(Set<String> securityIdSet, String startDate, String endDate, 
			Map<com.datastax.driver.core.LocalDate, Map<String, Double>> securityWeightByDate) throws Exception {
		
		String securities =  "'" + Joiner.on("','").join(securityIdSet) + "'";
		System.out.println(securities);
		
		String query = "select security_id, business_date, calc_daily_return from analyticspoc.security_factors_2 "
				+ " where security_id in (" + securities + ") "
				+ " and business_date >= '" + startDate + "'" + " and business_date <= '" + endDate + "'";
		
		long t1 = System.currentTimeMillis();
		final ResultSet rs = getSession().execute(query);
		long t2 = System.currentTimeMillis();
		System.out.println("Time taken query part 1: in sec: " + (t2-t1)/1000.0);
		
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
				portfolioReturn = portfolioReturn + (row.getDouble("calc_daily_return") * lookupMapForDate.get(securityId));
			}
			dateInPrevItr = businessDate;
			portfolioDayRet.put(businessDate.toString(), portfolioReturn);
			
			count2++;
		}
		
		long t3 = System.currentTimeMillis();
		System.out.println(String.format("Time taken query part 2: count: %s, in sec: %s ", count2, (t3-t2)/1000.0));
		return portfolioDayRet;
	}

	
	@Override
	public void startBatch() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endBatch() {
		// TODO Auto-generated method stub
		
	}
	
	public void lookupPortfolio(String portfolioId, String startDt, String endDt) throws Exception {
		
		List<Map<String, Object>> portfolioReturns = Lists.newArrayList();
		LocalDate startDate = LocalDate.parse(startDt);
		LocalDate endDate = LocalDate.parse(endDt);
		
		long numOfDaysBetween = ChronoUnit.DAYS.between(startDate, endDate); 
	    List<LocalDate> businessDays = IntStream.iterate(0, i -> i + 1)
	      .limit(numOfDaysBetween)
	      .mapToObj(i -> startDate.plusDays(i)).filter(date -> !(date.getDayOfWeek() == DayOfWeek.SATURDAY || date.getDayOfWeek() == DayOfWeek.SUNDAY))
	      .collect(Collectors.toList()); 
	    
	    final String qry = "select security_id, weight from analyticspoc.portfolio_positions where portfolio_id = '%s' and business_date = '%s'";
	    
	    long t1 = System.currentTimeMillis();
	    for (LocalDate businessDay : businessDays) {
			final ResultSet rs = getSession().execute(String.format(qry, portfolioId, businessDay));
			List<String> securityIds = Lists.newArrayList();
			List<Double> weights = Lists.newArrayList();
			for (Row row : rs) {
				securityIds.add(row.getString("security_id"));
				weights.add(row.getDouble("weight"));
			}
			
			Map<String, Object> dateReturnVal = getAggrFactors(securityIds.toArray(new String[securityIds.size()]), 
					ArrayUtils.toPrimitive(weights.toArray(new Double[weights.size()])), businessDay.toString());
			portfolioReturns.add(dateReturnVal);
		}
	    long t2 = System.currentTimeMillis();
	    System.out.println("Done.... rows: " + portfolioReturns.size() + " time in sec: " + (t2-t1)/1000.0);
	}
	
    public void lookupPortfolioEff(String portfolioId, String startDt, String endDt) throws Exception {
		
	    final String qry = "select business_date, security_id, weight from analyticspoc.portfolio_positions where "
	    		+ "portfolio_id = '%s' and business_date >= '%s' and business_date <= '%s'";
	    
	    long t1 = System.currentTimeMillis();
	    final ResultSet rs = getSession().execute(String.format(qry, portfolioId, startDt, endDt));
	    long t2 = System.currentTimeMillis();
	    System.out.println(String.format("Time taken query part 1: in sec: %s", (t2-t1)/1000.0));
	    
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
	    System.out.println(String.format("Time taken query part 1 processing: count: %s, in sec: %s", count1, (t3-t2)/1000.0));
	    
	    Map<String, Double> portfolioReturns = getAggrFactors(securitiesSet, startDt, endDt, securityWeightByDate);
	    
	    System.out.println("Done.... rows: " + portfolioReturns.size());
	}
	
	private Session session;
	private Cluster cluster;
	
	private Session getSession() {
		if(session == null) {
			Cluster.Builder clusterBuilder = Cluster.builder()
			    .addContactPoints(
			    		"127.0.0.1"
			        //"23.20.121.214", "34.195.86.103", "35.172.77.138" // AWS_VPC_US_EAST_1 (Amazon Web Services (VPC))
			    )
			    .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("AWS_VPC_US_EAST_1").build()) // your local data centre
			    .withPort(9042);
			    //.withAuthProvider(new PlainTextAuthProvider("iccassandra", "a832a593f4bd5098d30e77776c668c77"));
			Cluster cluster = clusterBuilder.build();
			session = cluster.connect();
		}
		return session;
	}
	
	public void connect() {
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

	public static void main(String[] args) {
		CassandraStoreLocal client = new CassandraStoreLocal();
		client.connect();
	
		try {
			/*Table results = client.getFactors(new String[] { "CVCO", "SCS", "BBG", "JRN", "JIVE", "BBT", "SCOR", "TWTC",
					"BBW", "BBY", "BBX", "SLAB", "FNLC", "BCC", "BCO", "CMRX", "BCR", "SEB", "SEE", "HCOM", "SEM",
					"BDC", "A", "B", "BDE", "CVEO", "NVEC", "C", "D", "STRT", "AGNC", "F", "G", "H", "STSA", "I", "K",
					"L", "BDN", "M", "N", "O", "P", "STSI", "CECO", "Q", "SLCA", "LGND", "R", "S", "SFE", "T", "SFG",
					"V", "BDX", "SFL", "X", "OPWR", "Y", "Z", "CECE", "BJRI", "NMRX", "NVDA", "BEE", "TWTR", "SFY",
					"BEN", "SGA", "TFSL", "VLGEA", "SGI", "SGK", "STRA", "SGM", "STRL", "CMTL", "SGY", "APAM", "SCSC",
					"DHIL", "BBBY", "CVGI", "ARTNA", "BFS", "SCSS", "SHO", "BGC", "JWN", "MSCC", "BGG", "SHW", "PBYI",
					"MSCI", "HCSG", "BGS", "SIG", "SIF", "BBCN", "CVGW", "BHB", "SIR", "BHE", "BSFT", "BHF", "BHI",
					"MSFG", "SIX", "HTWR", "STWD", "MSFT", "SJI", "SJM", "BID", "BSET", "BIG", "SJW", "BIO", "LXRX",
					"SKH", "SCTY", "JAKK", "MSEX", "SKT", "SKX", "GIFI", "FNSR", "SLB", "AGTC", "APEI", "HLIT", "SLH",
					"SLG", "SLM", "QNST", "BKD", "BKE", "BKH", "SCVL", "FFBC", "CVLT", "FFBH", "SLGN", "SMA", "BKS",
					"SMG", "BKU", "BKW", "PKOH", "SMP", "MBFI", "BBGI", "BLK", "PCAR", "CELG", "BLL", "SNA", "BLT",
					"SNH", "BLX", "SNI", "PTCT", "MBII", "PCBK", "SNV", "SNX", "BMI", "BMR", "BMS", "BMY", "SON",
					"RIGL", "SPB", "SPA", "PTEN", "SPF", "SPG", "ETFC", "SPN", "SPR", "BOH", "CEMP", "SPW", "GIII",
					"SQI", "VDSI", "KAI", "CENX", "PCCC", "VMEM", "YELP", "BPI", "KAR", "PTGI", "FFIN", "SRE", "SRI",
					"BPZ", "FFIV", "KBH", "KBR", "LPLA", "AGYS", "BBNK", "SSD", "SSI", "NEOG", "KCG", "SSP", "BRC",
					"NEON", "OHRP", "SSS", "CNBC", "FFIC", "GILD", "BRO", "CERN", "BRS", "STC", "BRT", "ECHO", "PTIE",
					"STE", "STJ", "FFKT", "STI", "STL", "SUBK", "STR", "STT", "STX", "STZ", "IOSP", "GIMO", "BSX",
					"SUI", "CNDO", "KEG", "SUN", "SUP", "HUBG", "KEM", "NNBR", "BBOX", "BTH", "CERS", "KEY", "KEX",
					"BTU", "HLSS", "BTX", "NETE", "BSRR", "APOL", "SVU", "APOG", "KFX", "PTLA", "SWC", "KFY", "SWI",
					"SWK", "SWM", "LPNT", "BBRG", "SWN", "SLRC", "SWS", "SWY", "SWX", "BKCC", "CETV", "SXC", "MSTR",
					"SXI", "KHC", "BBSI", "BWA", "SXT", "BWC", "PCLN", "CEVA", "SYA", "BSTC", "BWS", "SYF", "SYK",
					"FFNW", "BXC", "KIM", "MBRG", "KIN", "TOWR", "SYY", "SYX", "TOWN", "GABC", "BXP", "BXS" }, 
					new String[] {"open","close","high","low"}, "1999-01-01", "2019-01-01");
			
			client.saveFactors(results);*/
			
			/*List<Map<String, Object>> dataTab = client.getFactorsSimple(new String[] { "CVCO", "SCS", "BBG", "JRN", "JIVE", "BBT", "SCOR", "TWTC",
					"BBW", "BBY", "BBX", "SLAB", "FNLC", "BCC", "BCO", "CMRX", "BCR", "SEB", "SEE", "HCOM", "SEM",
					"BDC", "A", "B", "BDE", "CVEO", "NVEC", "C", "D", "STRT", "AGNC", "F", "G", "H", "STSA", "I", "K",
					"L", "BDN", "M", "N", "O", "P", "STSI", "CECO", "Q", "SLCA", "LGND", "R", "S", "SFE", "T", "SFG",
					"V", "BDX", "SFL", "X", "OPWR", "Y", "Z", "CECE", "BJRI", "NMRX", "NVDA", "BEE", "TWTR", "SFY",
					"BEN", "SGA", "TFSL", "VLGEA", "SGI", "SGK", "STRA", "SGM", "STRL", "CMTL", "SGY", "APAM", "SCSC",
					"DHIL", "BBBY", "CVGI", "ARTNA", "BFS", "SCSS", "SHO", "BGC", "JWN", "MSCC", "BGG", "SHW", "PBYI",
					"MSCI", "HCSG", "BGS", "SIG", "SIF", "BBCN", "CVGW", "BHB", "SIR", "BHE", "BSFT", "BHF", "BHI",
					"MSFG", "SIX", "HTWR", "STWD", "MSFT", "SJI", "SJM", "BID", "BSET", "BIG", "SJW", "BIO", "LXRX",
					"SKH", "SCTY", "JAKK", "MSEX", "SKT", "SKX", "GIFI", "FNSR", "SLB", "AGTC", "APEI", "HLIT", "SLH",
					"SLG", "SLM", "QNST", "BKD", "BKE", "BKH", "SCVL", "FFBC", "CVLT", "FFBH", "SLGN", "SMA", "BKS",
					"SMG", "BKU", "BKW", "PKOH", "SMP", "MBFI", "BBGI", "BLK", "PCAR", "CELG", "BLL", "SNA", "BLT",
					"SNH", "BLX", "SNI", "PTCT", "MBII", "PCBK", "SNV", "SNX", "BMI", "BMR", "BMS", "BMY", "SON",
					"RIGL", "SPB", "SPA", "PTEN", "SPF", "SPG", "ETFC", "SPN", "SPR", "BOH", "CEMP", "SPW", "GIII",
					"SQI", "VDSI", "KAI", "CENX", "PCCC", "VMEM", "YELP", "BPI", "KAR", "PTGI", "FFIN", "SRE", "SRI",
					"BPZ", "FFIV", "KBH", "KBR", "LPLA", "AGYS", "BBNK", "SSD", "SSI", "NEOG", "KCG", "SSP", "BRC",
					"NEON", "OHRP", "SSS", "CNBC", "FFIC", "GILD", "BRO", "CERN", "BRS", "STC", "BRT", "ECHO", "PTIE",
					"STE", "STJ", "FFKT", "STI", "STL", "SUBK", "STR", "STT", "STX", "STZ", "IOSP", "GIMO", "BSX",
					"SUI", "CNDO", "KEG", "SUN", "SUP", "HUBG", "KEM", "NNBR", "BBOX", "BTH", "CERS", "KEY", "KEX",
					"BTU", "HLSS", "BTX", "NETE", "BSRR", "APOL", "SVU", "APOG", "KFX", "PTLA", "SWC", "KFY", "SWI",
					"SWK", "SWM", "LPNT", "BBRG", "SWN", "SLRC", "SWS", "SWY", "SWX", "BKCC", "CETV", "SXC", "MSTR",
					"SXI", "KHC", "BBSI", "BWA", "SXT", "BWC", "PCLN", "CEVA", "SYA", "BSTC", "BWS", "SYF", "SYK",
					"FFNW", "BXC", "KIM", "MBRG", "KIN", "TOWR", "SYY", "SYX", "TOWN", "GABC", "BXP", "BXS" }, 
					new String[] {"open","close","high","low"}, new double[] {0.2, 0.2, 0.3, 0.3}, "1999-01-01", "2019-01-01");
			
			client.saveFactorSimple(dataTab);
			*/
			
			Table dataTab = client.getFactors(new String[] { "A", "AAL", "AAP", "AAPL",
					"ABBV", "ABC", "ABT", "ACN", "ADBE", "ADI", "ADM", "ADP", "ADS", "ADSK", "AEE", "AEP", "AES", "AET",
					"AFL", "AGN", "AIG", "AIV", "AIZ", "AJG", "AKAM", "ALB", "ALGN", "ALK", "ALL", "ALLE", "ALXN",
					"AMAT", "AMD", "AME", "AMG", "AMGN", "AMP", "AMT", "AMZN", "ANDV", "ANSS", "ANTM", "AON", "AOS",
					"APA", "APC", "APD", "APH", "APTV", "ARE", "ARNC", "ATVI", "AVB", "AVGO", "AVY", "AWK", "AXP",
					"AYI", "AZO", "BA", "BAC", "BAX", "BBT", "BBY", "BDX", "BEN", "BF.B", "BHF", "BHGE", "BIIB", "BK",
					"BKNG", "BLK", "BLL", "BMY", "BRK.B", "BSX", "BWA", "BXP", "C", "CA", "CAG", "CAH", "CAT", "CB",
					"CBG", "CBOE", "CBS", "CCI", "CCL", "CDNS", "CELG", "CERN", "CF", "CFG", "CHD", "CHK", "CHRW",
					"CHTR", "CI", "CINF", "CL", "CLX", "CMA", "CMCSA", "CME", "CMG", "CMI", "CMS", "CNC", "CNP", "COF",
					"COG", "COL", "COO", "COP", "COST", "COTY", "CPB", "CRM", "CSCO", "CSRA", "CSX", "CTAS", "CTL",
					"CTSH", "CTXS", "CVS", "CVX", "CXO", "D", "DAL", "DE", "DFS", "DG", "DGX", "DHI", "DHR", "DIS",
					"DISCA", "DISCK", "DISH", "DLR", "DLTR", "DOV", "DPS", "DRE", "DRI", "DTE", "DUK", "DVA", "DVN",
					"DWDP", "DXC", "EA", "EBAY", "ECL", "ED", "EFX", "EIX", "EL", "EMN", "EMR", "EOG", "EQIX", "EQR",
					"EQT", "ES", "ESRX", "ESS", "ETFC", "ETN", "ETR", "EVHC", "EW", "EXC", "EXPD", "EXPE", "EXR", "F",
					"FAST", "FB", "FBHS", "FCX", "FDX", "FE", "FFIV", "FIS", "FISV", "FITB", "FL", "FLIR", "FLR", "FLS",
					"FMC", "FOX", "FOXA", "FRT", "FTI", "FTV", "GD", "GE", "GGP", "GILD", "GIS", "GLW", "GM", "GOOG",
					"GOOGL", "GPC", "GPN", "GPS", "GRMN", "GS", "GT", "GWW", "HAL", "HAS", "HBAN", "HBI", "HCA", "HCP",
					"HD", "HES", "HIG", "HII", "HLT", "HOG", "HOLX", "HON", "HP", "HPE", "HPQ", "HRB", "HRL", "HRS",
					"HSIC", "HST", "HSY", "HUM", "IBM", "ICE", "IDXX", "IFF", "ILMN", "INCY", "INFO", "INTC", "INTU",
					"IP", "IPG", "IPGP", "IQV", "IR", "IRM", "ISRG", "IT", "ITW", "IVZ", "JBHT", "JCI", "JEC", "JNJ",
					"JNPR", "JPM", "JWN", "K", "KEY", "KHC", "KIM", "KLAC", "KMB", "KMI", "KMX", "KO", "KORS", "KR",
					"KSS", "KSU", "L", "LB", "LEG", "LEN", "LH", "LKQ", "LLL", "LLY", "LMT", "LNC", "LNT", "LOW",
					"LRCX", "LUK", "LUV", "LYB", "M", "MA", "MAA", "MAC", "MAR", "MAS", "MAT", "MCD", "MCHP", "MCK",
					"MCO", "MDLZ", "MDT", "MET", "MGM", "MHK", "MKC", "MLM", "MMC", "MMM", "MNST", "MO", "MON", "MOS",
					"MPC", "MRK", "MRO", "MS", "MSFT", "MSI", "MTB", "MTD", "MU", "MYL", "NAVI", "NBL", "NCLH", "NDAQ",
					"NEE", "NEM", "NFLX", "NFX", "NI", "NKE", "NLSN", "NOC", "NOV", "NRG", "NSC", "NTAP", "NTRS", "NUE",
					"NVDA", "NWL", "NWS", "NWSA", "O", "OKE", "OMC", "ORCL", "ORLY", "OXY", "PAYX", "PBCT", "PCAR",
					"PCG", "PDCO", "PEG", "PEP", "PFE", "PFG", "PG", "PGR", "PH", "PHM", "PKG", "PKI", "PLD", "PM",
					"PNC", "PNR", "PNW", "PPG", "PPL", "PRGO", "PRU", "PSA", "PSX", "PVH", "PWR", "PX", "PXD", "PYPL",
					"QCOM", "QRVO", "RCL", "RE", "REG", "REGN", "RF", "RHI", "RHT", "RJF", "RL", "RMD", "ROK", "ROP",
					"ROST", "RRC", "RSG", "RTN", "SBAC", "SBUX", "SCG", "SCHW", "SEE", "SHW", "SIG", "SJM", "SLB",
					"SLG", "SNA", "SNPS", "SO", "SPG", "SPGI", "SRCL", "SRE", "STI", "STT", "STX", "STZ", "SWK", "SWKS",
					"SYF", "SYK", "SYMC", "SYY", "T", "TAP", "TDG", "TEL", "TGT", "TIF", "TJX", "TMK", "TMO", "TPR",
					"TRIP", "TROW", "TRV", "TSCO", "TSN", "TSS", "TWX", "TXN", "TXT", "UA", "UAA", "UAL", "UDR", "UHS",
					"ULTA", "UNH", "UNM", "UNP", "UPS", "URI", "USB", "UTX", "V", "VAR", "VFC", "VIAB", "VLO", "VMC",
					"VNO", "VRSK", "VRSN", "VRTX", "VTR", "VZ", "WAT", "WBA", "WDC", "WEC", "WELL", "WFC", "WHR",
					"WLTW", "WM", "WMB", "WMT", "WRK", "WU", "WY", "WYN", "WYNN", "XEC", "XEL", "XL", "XLNX", "XOM",
					"XRAY", "XRX", "XYL", "YUM", "ZBH", "ZION", "ZTS" }, 
					new String[] {"open","close","high","low"}, "1999-01-01", "2018-01-01");
			
			client.saveFactors(dataTab);

			
			/*List<Map<String, Object>> dataTab = client.getFactorsSimple(new String[] { "A", "AAL", "AAP", "AAPL",
					"ABBV", "ABC", "ABT", "ACN", "ADBE", "ADI", "ADM", "ADP", "ADS", "ADSK", "AEE", "AEP", "AES", "AET",
					"AFL", "AGN", "AIG", "AIV", "AIZ", "AJG", "AKAM", "ALB", "ALGN", "ALK", "ALL", "ALLE", "ALXN",
					"AMAT", "AMD", "AME", "AMG", "AMGN", "AMP", "AMT", "AMZN", "ANDV", "ANSS", "ANTM", "AON", "AOS",
					"APA", "APC", "APD", "APH", "APTV", "ARE", "ARNC", "ATVI", "AVB", "AVGO", "AVY", "AWK", "AXP",
					"AYI", "AZO", "BA", "BAC", "BAX", "BBT", "BBY", "BDX", "BEN", "BF.B", "BHF", "BHGE", "BIIB", "BK",
					"BKNG", "BLK", "BLL", "BMY", "BRK.B", "BSX", "BWA", "BXP", "C", "CA", "CAG", "CAH", "CAT", "CB",
					"CBG", "CBOE", "CBS", "CCI", "CCL", "CDNS", "CELG", "CERN", "CF", "CFG", "CHD", "CHK", "CHRW",
					"CHTR", "CI", "CINF", "CL", "CLX", "CMA", "CMCSA", "CME", "CMG", "CMI", "CMS", "CNC", "CNP", "COF",
					"COG", "COL", "COO", "COP", "COST", "COTY", "CPB", "CRM", "CSCO", "CSRA", "CSX", "CTAS", "CTL",
					"CTSH", "CTXS", "CVS", "CVX", "CXO", "D", "DAL", "DE", "DFS", "DG", "DGX", "DHI", "DHR", "DIS",
					"DISCA", "DISCK", "DISH", "DLR", "DLTR", "DOV", "DPS", "DRE", "DRI", "DTE", "DUK", "DVA", "DVN",
					"DWDP", "DXC", "EA", "EBAY", "ECL", "ED", "EFX", "EIX", "EL", "EMN", "EMR", "EOG", "EQIX", "EQR",
					"EQT", "ES", "ESRX", "ESS", "ETFC", "ETN", "ETR", "EVHC", "EW", "EXC", "EXPD", "EXPE", "EXR", "F",
					"FAST", "FB", "FBHS", "FCX", "FDX", "FE", "FFIV", "FIS", "FISV", "FITB", "FL", "FLIR", "FLR", "FLS",
					"FMC", "FOX", "FOXA", "FRT", "FTI", "FTV", "GD", "GE", "GGP", "GILD", "GIS", "GLW", "GM", "GOOG",
					"GOOGL", "GPC", "GPN", "GPS", "GRMN", "GS", "GT", "GWW", "HAL", "HAS", "HBAN", "HBI", "HCA", "HCP",
					"HD", "HES", "HIG", "HII", "HLT", "HOG", "HOLX", "HON", "HP", "HPE", "HPQ", "HRB", "HRL", "HRS",
					"HSIC", "HST", "HSY", "HUM", "IBM", "ICE", "IDXX", "IFF", "ILMN", "INCY", "INFO", "INTC", "INTU",
					"IP", "IPG", "IPGP", "IQV", "IR", "IRM", "ISRG", "IT", "ITW", "IVZ", "JBHT", "JCI", "JEC", "JNJ",
					"JNPR", "JPM", "JWN", "K", "KEY", "KHC", "KIM", "KLAC", "KMB", "KMI", "KMX", "KO", "KORS", "KR",
					"KSS", "KSU", "L", "LB", "LEG", "LEN", "LH", "LKQ", "LLL", "LLY", "LMT", "LNC", "LNT", "LOW",
					"LRCX", "LUK", "LUV", "LYB", "M", "MA", "MAA", "MAC", "MAR", "MAS", "MAT", "MCD", "MCHP", "MCK",
					"MCO", "MDLZ", "MDT", "MET", "MGM", "MHK", "MKC", "MLM", "MMC", "MMM", "MNST", "MO", "MON", "MOS",
					"MPC", "MRK", "MRO", "MS", "MSFT", "MSI", "MTB", "MTD", "MU", "MYL", "NAVI", "NBL", "NCLH", "NDAQ",
					"NEE", "NEM", "NFLX", "NFX", "NI", "NKE", "NLSN", "NOC", "NOV", "NRG", "NSC", "NTAP", "NTRS", "NUE",
					"NVDA", "NWL", "NWS", "NWSA", "O", "OKE", "OMC", "ORCL", "ORLY", "OXY", "PAYX", "PBCT", "PCAR",
					"PCG", "PDCO", "PEG", "PEP", "PFE", "PFG", "PG", "PGR", "PH", "PHM", "PKG", "PKI", "PLD", "PM",
					"PNC", "PNR", "PNW", "PPG", "PPL", "PRGO", "PRU", "PSA", "PSX", "PVH", "PWR", "PX", "PXD", "PYPL",
					"QCOM", "QRVO", "RCL", "RE", "REG", "REGN", "RF", "RHI", "RHT", "RJF", "RL", "RMD", "ROK", "ROP",
					"ROST", "RRC", "RSG", "RTN", "SBAC", "SBUX", "SCG", "SCHW", "SEE", "SHW", "SIG", "SJM", "SLB",
					"SLG", "SNA", "SNPS", "SO", "SPG", "SPGI", "SRCL", "SRE", "STI", "STT", "STX", "STZ", "SWK", "SWKS",
					"SYF", "SYK", "SYMC", "SYY", "T", "TAP", "TDG", "TEL", "TGT", "TIF", "TJX", "TMK", "TMO", "TPR",
					"TRIP", "TROW", "TRV", "TSCO", "TSN", "TSS", "TWX", "TXN", "TXT", "UA", "UAA", "UAL", "UDR", "UHS",
					"ULTA", "UNH", "UNM", "UNP", "UPS", "URI", "USB", "UTX", "V", "VAR", "VFC", "VIAB", "VLO", "VMC",
					"VNO", "VRSK", "VRSN", "VRTX", "VTR", "VZ", "WAT", "WBA", "WDC", "WEC", "WELL", "WFC", "WHR",
					"WLTW", "WM", "WMB", "WMT", "WRK", "WU", "WY", "WYN", "WYNN", "XEC", "XEL", "XL", "XLNX", "XOM",
					"XRAY", "XRX", "XYL", "YUM", "ZBH", "ZION", "ZTS" }, 
					new String[] {"open","close","high","low"}, new double[] {0.2, 0.2, 0.3, 0.3}, "1999-01-01", "2018-01-01");
			
			client.saveFactorSimple(dataTab);
*/			
			//client.lookupPortfolio("SP500", "2017-01-01", "2018-01-01");
			client.lookupPortfolioEff("SP500", "1999-01-01", "2018-01-01");
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
	public void setLayout(String layout) {
		// TODO Auto-generated method stub
		
	}

}
