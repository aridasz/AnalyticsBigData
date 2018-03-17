package org.am.cdo.data.store;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.am.cdo.util.Utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import tech.tablesaw.api.Table;

public class RedisStore implements IDataStore{

	private Jedis jedis;
	private Pipeline pipeline;
	public RedisStore() {
      	jedis = new Jedis("localhost", 6379); 
      	System.out.println("Connection to server sucessfully"); 
      	//check whether server is running or not 
      System.out.println("Server is running: "+jedis.ping()); 
	}
/*	@Override
	public void saveFactor(String securityId, LocalDate businessDate, String factorName, float factorValue) {
		String key = "date:"+(businessDate.getYear()*10000+businessDate.getMonthValue()*100+businessDate.getDayOfMonth())
				+",security:"+securityId;
		//System.out.println("key="+key);
		jedis.hset(key, factorName, ""+factorValue);				
	}*/
	
	public void saveFactor(String securityId, LocalDate businessDate, Map<String,String> factors){
		String key = "date:"+(businessDate.getYear()*10000+businessDate.getMonthValue()*100+businessDate.getDayOfMonth())
				+",security:"+securityId;
		//System.out.println("key="+key);
		pipeline.hmset(key, factors);				
		//jedis.hmset(key, factors);				
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
		// TODO Auto-generated method stub
		return null;
	}

}
