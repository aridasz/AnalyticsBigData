package org.am.cdo.data.gen;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class DataCSVFormatter {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//convert("C:\\projects\\WIKI_PRICES_Sec\\WIKI_PRICES_Sec_Full.csv", "");
		//convert2("C:\\Vaibhav\\CassandraPoC\\SampleData\\US_EQ_Price_History_wide.csv", "C:\\Vaibhav\\CassandraPoC\\SampleData\\US_EQ_Price_History_long.csv");
		//convert2("C:\\Vaibhav\\CassandraPoC\\FullData\\SnP500_Price_History_wide.csv", "C:\\Vaibhav\\CassandraPoC\\FullData\\SnP500_Price_History_long.csv");
		convert2("C:\\Vaibhav\\CassandraPoC\\FullData\\US_EQ_Price_History_wide.csv", "C:\\Vaibhav\\CassandraPoC\\FullData\\US_EQ_Price_History_long.csv");
		convert2(args[0],args[1]);
	}

	
	public static void convert2(String inputFilename, String outputFileName)throws IOException {
		try (Stream<String> lines = Files.lines (Paths.get(inputFilename), Charset.defaultCharset()))
		{
			try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFileName))){
				String[] header =null;
				for (String line : (Iterable<String>) lines::iterator)
			    {
					String[] vals = line.split(",");
					if(header != null ) {
					for(int i=0; i < vals.length; i++) {
						if(i == 0 || i == 1) {
							continue;
						}
						writer.write(vals[0] + "," + vals[1] + "," + header[i] + "," + vals[i] + "\n");
					}
					}else {
						header = vals;
					}
			    }
			}
		}
	}
	
	public static void convert(String inputFilename, String outputFileName) throws IOException {
		String[] fact = {"secid","date","open","high","low","close","volume",
				"ex_dividend","split_ratio","adj_open","adj_high","adj_low","adj_close","adj_volume"};
		
		try (Stream<String> lines = Files.lines(Paths.get(inputFilename), Charset.defaultCharset())) {
			try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFileName)))
			{
				lines.forEachOrdered(line -> {
					try {
						if(!line.startsWith("ticker,date,")) {
							String[] vals = line.split(",");
							for(int i=0; i < vals.length; i++) {
								if(i == 0 || i == 1) {
									continue;
								}
								writer.write(vals[0] + "," + vals[1] + "," + fact[i] + "," + vals[i] + "\n");
							}
						}					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			}
		}
	}
}
