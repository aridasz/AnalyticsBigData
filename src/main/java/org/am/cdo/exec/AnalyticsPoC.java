package org.am.cdo.exec;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.am.cdo.data.gen.DataGenerator;
import org.am.cdo.data.store.CassandraStore;
import org.am.cdo.data.store.DummyStore;
import org.am.cdo.data.store.FileStore;
import org.am.cdo.data.store.IDataStore;
import org.am.cdo.data.store.Lookups;
import org.am.cdo.data.store.RedisStore;
import org.am.cdo.util.Utils;

public class AnalyticsPoC {

	public static void main(String[] args) { 
		
		String[] params = args;
		//Samples
		//String[] params = new String[]{"-m","generate","-t","factors","-d","file","-y","2","-u","100","-f","10", "-s","200801"};
		//String[] params = new String[]{"-m","generate","-t","positions","-d","file","-y","5","-u","100","-h","50","-p","1", "-s","200801"};
		
		//Full
		
		//String[] params = new String[]{"-m","generate","-t","factors","-d","file","-y","10","-u","1500","-f","10", "-s","200801"};
		//String[] params = new String[]{"-m","generate","-t","positions","-d","file","-y","10","-u","1500","-h","500","-p","1", "-s","200801"};
		//String[] params = new String[]{"-m","generate","-t","positions","-d","file","-y","10","-u","1500","-h","100","-p","2", "-s","200801"};
		//String[] params = new String[]{"-m","generate","-t","positions","-d","file","-y","10","-u","1500","-h","500","-p","1", "-s","200801"};
		//String[] params = new String[]{"-m","generate","-t","factors","-d","file","-y","1","-u","5","-f","10", "-s","200801"};
		//String[] params = new String[]{"-m","query", "-d", "cassandra"};
		HashMap<String,String> parameters = new HashMap<String,String>();
		System.out.println(params.length);
		for(int i=0;i<params.length;i=i+2){
			parameters.put(params[i], params[i+1]);
		}
		System.out.println(parameters);

		if("generate".equals(parameters.get("-m"))){
			String dataSet = parameters.get("-t");
			String storeType = parameters.get("-d");
			int yearsHistory = Integer.parseInt(parameters.get("-y"));
			int universeSize = Integer.parseInt(parameters.get("-u"));
			int startDate = Integer.parseInt(parameters.get("-s"));
			String layout = parameters.get("-l");
			DataGenerator dg = new DataGenerator();
			IDataStore dataStore = getDataStore(storeType);
			if(layout != null) {
				dataStore.setLayout(layout);
			}
			if("factors".equals(dataSet)){
				int noFactors = Integer.parseInt(parameters.get("-f"));
				System.out.println("Generating "+dataSet+" yearsHistory="+yearsHistory+" universeSize="+universeSize+" noFactors="+noFactors);
				//dg.loadFactors(yearsHistory, universeSize, noFactors, dataStore);
				dg.loadFactors(yearsHistory, universeSize, noFactors, startDate, dataStore);
			}else if ("positions".equals(dataSet)){
				String portfolioId = parameters.get("-p");
				int noSecurity = Integer.parseInt(parameters.get("-h"));
				System.out.println("Generating "+dataSet+" yearsHistory="+yearsHistory+" universeSize="+universeSize+" noSecurity="+noSecurity);
				dg.loadPositions(portfolioId, yearsHistory, noSecurity, universeSize, startDate, dataStore);
			}
		}else if ("query".equals(parameters.get("-m"))){
			String dataSet = parameters.get("-t");
			String storeType = parameters.get("-d");
			IDataStore dataStore = getDataStore(storeType);
			getFactors(5, 5, 30, LocalDate.of(2009, 1, 1), 1500, dataStore);
			getFactors(5, 5, 30, LocalDate.of(2009, 1, 1), 1500, dataStore);
			getFactors(100, 5, 30, LocalDate.of(2009, 1, 1), 1500, dataStore);
			getFactors(100, 5, 365, LocalDate.of(2009, 1, 1), 1500, dataStore);
			getFactors(500, 5, 30, LocalDate.of(2009, 1, 1), 1500, dataStore);
			getFactors(500, 5, 365, LocalDate.of(2009, 1, 1), 1500, dataStore);
			getFactors(1000, 5, 30, LocalDate.of(2009, 1, 1), 1500, dataStore);
			getFactors(1000, 5, 365, LocalDate.of(2009, 1, 1), 1500, dataStore);
			}
		
		return;
		
		//IDataStore dataStore = new FileStore();
		//IDataStore dataStore = new RedisStore();
		//IDataStore dataStore = new DummyDataStore();
		//DataGenerator dg = new DataGenerator();
		//dg.loadFactors(1, 100, 10, dataStore);
//		dg.loadFactors(1, 100, 50, dataStore);
//		dg.loadFactors(1, 100, 100, dataStore);
//		dg.loadFactors(1, 100, 150, dataStore);
//		
//		dg.loadFactors(3, 1000, 10, dataStore);
//		dg.loadFactors(3, 1000, 50, dataStore);
//		dg.loadFactors(3, 1000, 100, dataStore);
//		dg.loadFactors(3, 1000, 150, dataStore);

		//dg.loadFactors(5, 1500, 10, dataStore);
		//dg.loadFactors(5, 1500, 50, dataStore);
		//dg.loadFactors(5, 1500, 100, dataStore);
		//dg.loadFactors(5, 1500, 150, dataStore);

		//dg.loadFactors(1, 1500, 140, dataStore);
		//dg.loadFactors(1, 100, 5, dataStore);
		//dg.loadPositions(1, 5, 10, 1500, dataStore);
		// dg.loadPositions(2, 5, 100, 1500, dataStore);
		//dg.loadPositions(3, 5, 500, 1500, dataStore);
		//dg.loadPositions(4, 5, 1000, 1500, dataStore);
		//dg.loadPositions(5, 5, 1500, 1500, dataStore);
		
		//loadFactors years=1 sec=100 factots=5 timetaken=00:00:14.031
		//loadFactors years=5 sec=1 factots=140 timetaken=00:00:16.990

//		getFactors(1, 1, 30, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1, 1, 365, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1, 1, 365*3, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1, 1, 365*5, LocalDate.of(2018, 1,31), 1500, dataStore);
//		
//		getFactors(100, 1, 30, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(100, 1, 365, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(100, 1, 365*3, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(100, 1, 365*5, LocalDate.of(2018, 1,31), 1500, dataStore);
//		
//		getFactors(500, 1, 30, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(500, 1, 365, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(500, 1, 365*3, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(500, 1, 365*5, LocalDate.of(2018, 1,31), 1500, dataStore);
//
//		getFactors(1000, 1, 30, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1000, 1, 365, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1000, 1, 365*3, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1000, 1, 365*5, LocalDate.of(2018, 1,31), 1500, dataStore);
//		
//		getFactors(1500, 1, 30, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1500, 1, 365, LocalDate.of(2018, 1,31), 1500, dataStore);
//		getFactors(1500, 1, 365*3, LocalDate.of(2018, 1,31), 1500, dataStore);
		//getFactors(1500, 100, 365, LocalDate.of(2018, 1,31), 1500, dataStore);

	}
	
	public static void getFactors (int noSec, int noFactors, int noDays, LocalDate startDate, int universeSize, IDataStore dataStore){
		int secIds[] = ThreadLocalRandom.current().ints(1, universeSize).distinct().limit(noSec).toArray();
		List<String> factors = new ArrayList<String>();
		for(int i=1;i<=noFactors;i++){
			factors.add("Factor"+i);
		}
		long startTime = System.currentTimeMillis();
		String[] a=  Arrays.stream(secIds).mapToObj(x -> Lookups.Tickers[x]).toArray(String[]::new);
		dataStore.getFactors(a, factors.toArray(new String[factors.size()]), startDate, startDate.plusDays(noDays));
		long endTime = System.currentTimeMillis();
		System.out.println("getFactors noSec="+noSec+" noDays="+ noDays+" timetaken="+Utils.formatTime(endTime-startTime));
	}
	
	public static IDataStore getDataStore(String type){
		if("file".equals(type)){
			return new FileStore();
		}
		if("redis".equals(type)){
			return new RedisStore();
		}
		if("cassandra".equals(type)){
			return new CassandraStore();
		}
		if("dummy".equals(type)){
			return new DummyStore();
		}
		throw new RuntimeException("Invalid Store type : "+type);
	}
	
	public static void printUsage(){
		System.out.println("-m mode : ex generate,query"); 
		System.out.println("-t data type : ex factors,positions"); 
		System.out.println("-d data store : ex file,redis,cassandra");
		System.out.println("-y history in years : ex 10");
		System.out.println("-u security universe size : ex 1000");
		System.out.println("-f no of factors : ex 140");
		System.out.println(" # required only for positions");
		System.out.println("-p portfolioId : ex 1,2,3");
		System.out.println(" # required only for positions");
		System.out.println("-h no of holding per day : ex 500");
		System.out.println(" # required only for positions");
		System.out.println("-s start date month : ex 200801");
		System.out.println("-l layout : ex wide,long");
		
		//System.out.println("-e end date month : ex 200801");
		
	}
}
