package org.am.cdo.data.gen;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.am.cdo.data.store.IDataStore;
import org.am.cdo.data.store.Lookups;
import org.am.cdo.util.Utils;

public class DataGenerator {
	
	
	public void loadPositions(String portfolioId, int noYears, int noSecurity, int universeSize, int currentDate, IDataStore dataStore) {
		System.out.println("loadPositions start portfolioId="+portfolioId+" years="+noYears+" sec="+noSecurity);
		long startTime = System.currentTimeMillis();
		LocalDate startDate = LocalDate.of(currentDate/100, currentDate%100, 1);
		LocalDate endDate = startDate.plusYears(noYears);
		while(startDate.isBefore(endDate)) {
			int secIds[] = ThreadLocalRandom.current().ints(1, universeSize).distinct().limit(noSecurity).toArray();
			List<Float> weights = generateWeights(noSecurity);
			for(int i=0;i<noSecurity;i++) {
				dataStore.savePosition(portfolioId, startDate, Lookups.Tickers[secIds[i]], weights.get(i));
			}
			startDate = startDate.plusDays(1);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("loadPositions end portfolioId="+portfolioId+" years="+noYears+" sec="+noSecurity+" timetaken="+Utils.formatTime(endTime-startTime));
	}
	
	public  void loadFactors(int noYears, int noSecurity, int noFactors, int currentDate, IDataStore dataStore) {
		System.out.println("loadFactors Start years="+noYears+" sec="+noSecurity+" factots="+noFactors);
		long startTime = System.currentTimeMillis();
		LocalDate startDate = LocalDate.of(currentDate/100, currentDate%100, 1);
		LocalDate endDate = startDate.plusYears(noYears);
		while(startDate.isBefore(endDate)) {
			dataStore.startBatch();
			long batchStart = System.currentTimeMillis();
			for(int s=1;s<=noSecurity;s++) {
				HashMap<String, String> map = new HashMap<String, String>();
				for(int f=1;f<=noFactors;f++) {
					map.put("Factor"+f, ""+(float)(ThreadLocalRandom.current().nextDouble(0,1)));
				}
				dataStore.saveFactor(Lookups.Tickers[s], startDate, map);
			}
			dataStore.endBatch();
			//System.out.println(" batch year=" + startDate+" timetaken="+Utils.formatTime(System.currentTimeMillis()-batchStart));
			startDate = startDate.plusDays(1);			
		}
		long endTime = System.currentTimeMillis();
		System.out.println("loadFactors End years="+noYears+" sec="+noSecurity+" factots="+noFactors+" timetaken="+Utils.formatTime(endTime-startTime));
	}
	
/*	public  void loadFactors(int noYears, int noSecurity, int noFactors, IDataStore dataStore) {
		System.out.println("loadFactors Start years="+noYears+" sec="+noSecurity+" factots="+noFactors);
		long startTime = System.currentTimeMillis();
		LocalDate startDate = LocalDate.now().minusYears(noYears);
		LocalDate endDate = LocalDate.now();
		while(startDate.isBefore(endDate)) {
			dataStore.startBatch();
			long batchStart = System.currentTimeMillis();
			for(int s=1;s<=noSecurity;s++) {
				HashMap<String, String> map = new HashMap<String, String>();
				for(int f=1;f<=noFactors;f++) {
					map.put("Factor"+f, ""+randFloat(0,1));
				}
				dataStore.saveFactor(s, startDate, map);
			}
			dataStore.endBatch();
			//System.out.println(" batch year=" + startDate+" timetaken="+Utils.formatTime(System.currentTimeMillis()-batchStart));
			startDate = startDate.plusDays(1);			
		}
		long endTime = System.currentTimeMillis();
		System.out.println("loadFactors End years="+noYears+" sec="+noSecurity+" factots="+noFactors+" timetaken="+Utils.formatTime(endTime-startTime));
	}
*/	
	
/*	public  void loadFactors2(int noYears, int noSecurity, int noFactors, IDataStore dataStore) {
		long startTime = System.currentTimeMillis();
		LocalDate startDate = LocalDate.now().minusYears(noYears);
		LocalDate endDate = LocalDate.now();
		while(startDate.isBefore(endDate)) {
			startDate = startDate.plusDays(1);
			for(int s=1;s<=noSecurity;s++) {
				for(int f=1;f<=noFactors;f++) {
					dataStore.saveFactor(s, startDate, "Factor"+f, randFloat(0,1));
				}
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("loadFactors years="+noYears+" sec="+noSecurity+" factots="+noFactors+" timetaken="+Utils.formatTime(endTime-startTime));
	}
*/
	private List<Float> generateWeights(int n) {
		
		List<Float> list = new ArrayList<Float>();
		float sum=0;
		for(int i=0;i<n; i++) {
			list.add((float)ThreadLocalRandom.current().nextDouble(0,1));
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
}
