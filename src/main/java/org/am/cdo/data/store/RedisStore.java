package org.am.cdo.data.store;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.am.cdo.util.Utils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import tech.tablesaw.api.Table;

public class RedisStore implements IDataStore{

	private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	private Jedis jedis;
	private Pipeline pipeline;
	private JedisCluster jedisCluster;
	
	
	private JedisPoolConfig buildPoolConfig() {
	    final JedisPoolConfig poolConfig = new JedisPoolConfig();
	    poolConfig.setMaxTotal(128);
	    poolConfig.setMaxIdle(128);
	    poolConfig.setMinIdle(16);
	    poolConfig.setTestOnBorrow(true);
	    poolConfig.setTestOnReturn(true);
	    poolConfig.setTestWhileIdle(true);
	    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
	    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
	    poolConfig.setNumTestsPerEvictionRun(3);
	    poolConfig.setBlockWhenExhausted(true);
	    return poolConfig;
	}
	
	/*public RedisStore() {
      	jedis = new Jedis("localhost", 6379); 
      	System.out.println("Connection to server sucessfully"); 
      	//check whether server is running or not 
       System.out.println("Server is running: "+jedis.ping()); 
	}*/
	
    /*@Override
	public void saveFactor(String securityId, LocalDate businessDate, String factorName, float factorValue) {
		String key = "date:"+(businessDate.getYear()*10000+businessDate.getMonthValue()*100+businessDate.getDayOfMonth())
				+",security:"+securityId;
		//System.out.println("key="+key);
		jedis.hset(key, factorName, ""+factorValue);				
	}*/
	
	public void saveFactor(String securityId, LocalDate businessDate, Map<String,String> factors) {
		String key = "date:"+(businessDate.getYear()*10000+businessDate.getMonthValue()*100+businessDate.getDayOfMonth())
				+",security:"+securityId;
		//System.out.println("key="+key);
		pipeline.hmset(key, factors);				
		//jedis.hmset(key, factors);	
	}

	public void saveFactor(String securityId, String businessDate, Map<String,String> factorsData) {
		startBatch();
		
		double score = ZonedDateTime.parse(businessDate, dateFormatter).toInstant().toEpochMilli();
		for (Entry<String, String> factor : factorsData.entrySet()) {
			pipeline.zadd("security_factor:" + securityId + "_" + factor.getKey(), score, factor.getValue());
		}
		
		endBatch();
	}

	@Override
	public void savePosition(String portfolioId, LocalDate businessDate, String securityId, double weight) {
		String key = "date:"+(businessDate.getYear()*10000+businessDate.getMonthValue()*100+businessDate.getDayOfMonth())
				+",portfolio:"+portfolioId;
		//System.out.println("key="+key+" value="+value);
		jedis.hset(key, "security:"+securityId, ""+weight);				
	}
	
	
	public List<List<String>> getFactors(String[] securityIds, String[] factorNames, LocalDate startDate, LocalDate endDate) {
		long days = startDate.until(endDate, ChronoUnit.DAYS) + 1;
		long startTime = System.currentTimeMillis();
		Pipeline p = jedis.pipelined();
		List<Response<List<String>>> responses = new ArrayList<Response<List<String>>>();
		while(startDate.isBefore(endDate)) {
			startDate = startDate.plusDays(1);
			for(int s=0;s<securityIds.length;s++) {
				String key = "date:"+(startDate.getYear()*10000+startDate.getMonthValue()*100+startDate.getDayOfMonth())
						+",security:"+securityIds[s];
				//System.out.println(key);
				responses.add(p.hmget(key, factorNames));
			}
		}
		p.sync();
		List<List<String>>  factors = new ArrayList<List<String>>(); 
		for(Response<List<String>> r : responses) {
			factors.add(r.get());
			//System.out.println(r.get()+"  "+r.get().getClass());
		}
		long endTime = System.currentTimeMillis();
		System.out.println("getFactors days="+days+" sec="+securityIds.length+" factors="+factorNames.length+" rows="+factors.size()+" timetaken="+Utils.formatTime(endTime-startTime));
		return factors;
	}
	
	public void startBatch(){
		pipeline = jedis.pipelined();	
	}

	public void endBatch(){
		pipeline.sync();
	}
	
	public void setLayout(String layout) {
		
	}

	@Override
	public Table getFactors(String[] securityIds, String[] factors, String startDate, String endDate) throws Exception {
		
		startBatch();
		
		for (String secId : securityIds) {
			for (String factor : factors) {
				pipeline.zrange("security_factor:" + secId + "_" + factor, 
						ZonedDateTime.parse(startDate, dateFormatter).toInstant().toEpochMilli(), 
						ZonedDateTime.parse(endDate, dateFormatter).toInstant().toEpochMilli())
				.get();
			}
		}
		
		
		String factorNames = Arrays.stream(factors).collect(Collectors.joining(","));
		long t1 = System.currentTimeMillis();
		
		String query = "select security_id, business_date, " + factorNames + " from amcdopoc.security_factors "
				+ " where security_id = ? and business_date >= '" + startDate + "' and business_date <= '" + endDate + "'";
		
		Table table = extractResults(sendQueries(session, session.prepare(query), securityIds), factors);
		
		long t2 = System.currentTimeMillis();
		System.out.println("--- all done ---- count: " + table.rowCount() + " in sec: " + (t2-t1)/1000.0);
		System.out.println(table.selectWhere(table.categoryColumn("security_id").isEqualTo("MSFT")).print());
		
		return table;
	}

	@Override
	public void lookupPortfolio(String portfolioId, String startDate, String endDate) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		jedis.close();
	}

	@Override
	public void saveFactors(int batchSize, Table dataTab) throws Exception {
		
	}

	@Override
	public void connect() {
		this.jedisCluster = new JedisCluster(new HostAndPort("localhost", 6379), buildPoolConfig());
		this.jedis = new Jedis("localhost", 6379); 
      	System.out.println("Server is running: " + jedis.ping());
	}

}
