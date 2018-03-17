package org.am.cdo.conn.test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassConnector {

	/** Cassandra Cluster. */
	   private Cluster cluster;
	   /** Cassandra Session. */
	   private Session session;
	   /**
	    * Connect to Cassandra Cluster specified by provided node IP
	    * address and port number.
	    *
	    * @param node Cluster node IP address.
	    * @param port Port of cluster host.
	    */
	   public void connect(final String node, final int port)
	   {
	      this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
	      final Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
	      for (final Host host : metadata.getAllHosts())
	      {
	         System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
	            host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	      session = cluster.connect();
	      System.out.println("connected..........");
	   }
	   /**
	    * Provide my Session.
	    *
	    * @return My session.
	    */
	   public Session getSession()
	   {
	      return this.session;
	   }
	   /** Close cluster. */
	   public void close()
	   {
	      cluster.close();
	   }
	
	public static void main(String[] args) throws Exception {
		CassConnector client = new CassConnector();
		
		client.connect("127.0.0.1", 9042);
		
		exec(client, "select business_date, open from analyticspoc.security_factors_2 "
				+ " where security_id in ('AMZN','A','GOOG','AGO','ALG','AMD','AMSC',"
				+ "'ANR','ANF','AON','APD','APOL','AROW','ASH','ATNI','AVAV','AVT','AXS','ATU','AXE','AZZ','BAC',"
				+ "'BBT','BC','BDGE','BEAM','BHI','BOH','CAT','CCO','CERS','CLC','CKEC','CIFC','CLFD','CMA','CMN',"
				+ "'COF','COHU','CONN','CRUS','CUI','CVGI','CWEI','CY','CVX','CYT','DAVE','D','DCO','DEPO','DHI',"
				+ "'DORM','DNB','DWS','ELLI','ENZ','EPAM','EQR') "
				+ " and business_date > '1999-01-01' and business_date < '2019-01-01'"
				, " select business_date, open where security_id, date");
		
		exec(client, "select business_date, factor_value from analyticspoc.security_factors_1 "
				+ " where security_id in ('AMZN','A','GOOG','AGO','ALG','AMD','AMSC',"
				+ "'ANR','ANF','AON','APD','APOL','AROW','ASH','ATNI','AVAV','AVT','AXS','ATU','AXE','AZZ','BAC',"
				+ "'BBT','BC','BDGE','BEAM','BHI','BOH','CAT','CCO','CERS','CLC','CKEC','CIFC','CLFD','CMA','CMN',"
				+ "'COF','COHU','CONN','CRUS','CUI','CVGI','CWEI','CY','CVX','CYT','DAVE','D','DCO','DEPO','DHI',"
				+ "'DORM','DNB','DWS','ELLI','ENZ','EPAM','EQR') "
				+ " and factor_name in ('open','close') "
				+ " and business_date > '1999-01-01' and business_date < '2019-01-01'"
				, " select business_date, factor_value where security_id, factor_name, date");
		
		execMulti(client);
		
		client.close();
	}

	private static void exec(CassConnector client, String query, String strategysummary) {
		System.out.println("############" + strategysummary + "##############");
		long t1 = System.currentTimeMillis();
		final ResultSet rs = client.getSession().execute(query);
		long t2 = System.currentTimeMillis();
		System.out.println("--- got results---- in sec: " + (t2-t1)/1000.0);		
		int count = 0;
		for(Row row : rs) {
			count++;
			//System.out.println(row.getDate("business_date") + "|" + row.getDouble("open"));
			row.getDate(0);
			row.getDouble(1);			
		}
		long t3 = System.currentTimeMillis();
		System.out.println("done, count: in sec: " + count + " | " + (t3-t2)/1000.0);
	}
	
	private static void execMulti(final CassConnector client) throws Exception {
		ExecutorService executorService = Executors.newFixedThreadPool(4);
		CompletionService executorCompletionService= new ExecutorCompletionService<>(executorService );
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		final String qry = "select business_date, factor_value from analyticspoc.security_factors_1 "
				+ " where security_id in ('AMZN','A','GOOG','AGO','ALG','AMD','AMSC',"
				+ "'ANR','ANF','AON','APD','APOL','AROW','ASH','ATNI','AVAV','AVT','AXS','ATU','AXE','AZZ','BAC',"
				+ "'BBT','BC','BDGE','BEAM','BHI','BOH','CAT','CCO','CERS','CLC','CKEC','CIFC','CLFD','CMA','CMN',"
				+ "'COF','COHU','CONN','CRUS','CUI','CVGI','CWEI','CY','CVX','CYT','DAVE','D','DCO','DEPO','DHI',"
				+ "'DORM','DNB','DWS','ELLI','ENZ','EPAM','EQR') "
				+ " and factor_name = ? "
				+ " and business_date > '1999-01-01' and business_date < '2019-01-01'";
		
		String[] factors = {"open", "close", "high", "low"};
		
		long t1 = System.currentTimeMillis();
		
		for (String f : factors) {
			final String factor = f;
			futures.add(executorCompletionService.submit(new Callable<Integer>() {

				@Override
				public Integer call() throws Exception {				
					final ResultSet rs = client.getSession().execute(qry, factor);
					int count = 0;
					for(Row row : rs) {
						count++;
						row.getDate(0);
						row.getDouble(1);			
					}
					
					return count;
				}
			}));
		}
		
		int count = 0;
		for (Future<Integer> fut : futures) {
			count = count + fut.get();
		}	
		
		long t2 = System.currentTimeMillis();
		System.out.println("done, count: in sec: " + count + " | " + (t2-t1)/1000.0);
		executorService.shutdown();
	}
}
