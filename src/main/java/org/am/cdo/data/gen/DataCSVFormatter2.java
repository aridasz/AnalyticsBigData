package org.am.cdo.data.gen;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

public class DataCSVFormatter2 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		convert("C:\\projects\\WIKI_PRICES_Sec\\WIKI_PRICES_Sec_Full.csv");
		//convert2("/home/ec2-user/notebooks/SP3000_constituents.csv");

	}

	public static void convert(String filename) throws IOException {
		String[] fact = {"secid","date","open","high","low","close","volume",
				"ex_dividend","split_ratio","adj_open","adj_high","adj_low","adj_close","adj_volume"};
		Set<String> secIds = new HashSet<String>();
		
		try (Stream<String> lines = Files.lines(Paths.get(filename), Charset.defaultCharset())) {
			try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("C:\\\\projects\\\\WIKI_PRICES_Sec\\\\WIKI_PRICES_sec_by_year.csv")))
			{
				lines.forEachOrdered(line -> {
					try {
						if(!line.startsWith("ticker,date,")) {
							String[] vals = line.split(",");
							for(int i=0; i < vals.length; i++) {
								if(i == 0 || i == 1) {
									continue;
								}
								secIds.add(vals[0]);
								//writer.write(vals[0] + "," + vals[1] + "," + fact[i] + "," + vals[i] + "\n");
							}
						}
						String[] vals = line.split(",");
						writer.write(vals[1].split("-")[0] + "," + line + "\n");
						
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		}
		//System.out.println(secIds);
	}
	
	public static void convert2(String filename) throws IOException {
		LocalDate startDate = LocalDate.parse("1999-01-01");
		LocalDate endDate = LocalDate.parse("2019-01-01");

		long numOfDaysBetween = ChronoUnit.DAYS.between(startDate, endDate); 
	    List<LocalDate> businessDays = IntStream.iterate(0, i -> i + 1)
	      .limit(numOfDaysBetween)
	      .mapToObj(i -> startDate.plusDays(i)).filter(date -> !(date.getDayOfWeek() == DayOfWeek.SATURDAY || date.getDayOfWeek() == DayOfWeek.SUNDAY))
	      .collect(Collectors.toList());
	    
		try (Stream<String> lines = Files.lines(Paths.get(filename), Charset.defaultCharset())) {
			try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("/home/ec2-user/notebooks/SP1000_constituents_gen.csv")))
			{
				writer.write("portfolio_id,business_date,security_id,weight\n"); 
				List<String> linesList = Lists.newArrayList();
				lines.forEachOrdered(line -> {
					try {
						linesList.add(line);							
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
				for (LocalDate date : businessDays) {
					int count = 1;
					for (String line : linesList) {
						writer.write("SP1000" + "," + date.toString() + "," + line + "\n");
						if(count == 1000) break;
						count++;
					}
				}
			}
		}
	}
}
