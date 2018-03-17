package org.am.cdo.conn.test;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.am.cdo.util.Utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class Test {
	

	public static void main(String[] args) { 
		System.out.println(Utils.formatTime(14031));
	      //Connecting to Redis server on localhost 
	      	//Jedis jedis = new Jedis("localhost", 6379); 
	      	//System.out.println("Connection to server sucessfully"); 
	      	//check whether server is running or not 
	      //System.out.println("Server is running: "+jedis.ping()); 
	      //loadFactors(1,1,140, jedis);
	      //sloadPositions("1",1,2,5, jedis);
	      
//		ThreadLocalRandom.current().ints(0, 100).distinct().limit(5).forEach(System.out::println);
//
//		System.out.println(generateWeights(2));
//		System.out.println(generateWeights(10));
		//System.out.println(LocalDate.of(2017, 10, 21).until(LocalDate.of(2017, 10, 22)).get(ChronoUnit.DAYS));
		//System.out.println(LocalDate.of(2017, 10, 21).until(LocalDate.of(2017, 10, 22), ChronoUnit.DAYS));
		//getFactors(new int[]{1,2}, new String[]{"Factor1","Factor2"}, LocalDate.of(2017, 10, 21), LocalDate.of(2017, 10, 22));
		int currentDate = 201701;
		int noYears =1;
		LocalDate startDate = LocalDate.of(currentDate/100, currentDate%100, 1);
		LocalDate endDate = startDate.plusYears(noYears);
		System.out.println(startDate);
		System.out.println(endDate);
	   } 
	
	public static List<List<String>> getFactors(int[] securityIds, String[] factorNames, LocalDate startDate, LocalDate endDate) {
		long days = startDate.until(endDate, ChronoUnit.DAYS) + 1;
		Jedis jedis = new Jedis("localhost", 6379);
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
		System.out.println("getFactors days="+days+" sec="+securityIds.length+" factors="+factorNames.length+" timetaken="+Utils.formatTime(endTime-startTime));
		return factors;
	}
	
	public static void loadFactors(int noYears, int noSecurity, int noFactors, Jedis jedis) {
		long startTime = System.currentTimeMillis();
		LocalDate startDate = LocalDate.now().minusYears(noYears);
		LocalDate endDate = LocalDate.now();
		while(startDate.isBefore(endDate)) {
			startDate = startDate.plusDays(1);
			for(int s=1;s<=noSecurity;s++) {
				for(int f=1;f<=noFactors;f++) {
					String key = "date:"+(startDate.getYear()*10000+startDate.getMonthValue()*100+startDate.getDayOfMonth())
							+",security:"+s;
					String value = ""+randFloat(0,1);
					System.out.println("key="+key+" value="+value);
					jedis.hset(key, "Factor"+f, value);				
					}
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("loadFactors years="+noYears+" sec="+noSecurity+" factots="+noFactors+" timetaken="+(endTime-startTime));
		//logger.log(Level.INFO, "loadFactors years="+noYears+" sec="+noSecurity+" factots="+noFactors+" timetaken="+(endTime-startTime));
	}
	
	
	public static void loadPositions(String portfolioId, int noYears, int noSecurity, int universeSize, Jedis jedis) {
		long startTime = System.currentTimeMillis();
		LocalDate startDate = LocalDate.now().minusYears(noYears);
		LocalDate endDate = LocalDate.now();
		while(startDate.isBefore(endDate)) {
			startDate = startDate.plusDays(1);
			int secIds[] = ThreadLocalRandom.current().ints(1, universeSize).distinct().limit(noSecurity).toArray();
			List<Float> weights = generateWeights(noSecurity);
			for(int i=0;i<noSecurity;i++) {
				String key = "date:"+(startDate.getYear()*10000+startDate.getMonthValue()*100+startDate.getDayOfMonth())
						+",portfolio:"+portfolioId;
				String value = ""+weights.get(i);
				//System.out.println("key="+key+" value="+value);
				jedis.hset(key, "security:"+secIds[i], value);				
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("loadPositions years="+noYears+" sec="+noSecurity+" timetaken="+(endTime-startTime));
	}
	
	public static List<Float> generateWeights(int n) {
		List<Float> list = new ArrayList<Float>();
		float sum=0;
		for(int i=0;i<n; i++) {
			list.add(randFloat(0,1));
			sum += list.get(i);
		}
		float total = 0;
		for(int i=0;i<n;i++) {
			list.set(i, list.get(i)/sum);
			total += list.get(i);
		}
		//System.out.println("Total="+total);
		return list;
	}
	
	public static float randFloat(float min, float max) {

	    Random rand = new Random();

	    float result = rand.nextFloat() * (max - min) + min;

	    return result;

	}

}
