package org.am.cdo.exec;
import org.am.cdo.util.FileUtils;
import org.am.cdo.util.Utils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

public class RunCQLFile {
	private Cluster cluster;
	private Session session;

	public static void main(String[] args) {
		String cqlFile = args[0];//"C:\\Vaibhav\\queries.cql";
		RunCQLFile r = new RunCQLFile();
		r.connect();
		r.run("SELECT cql_version FROM system.local");
		r.execute(cqlFile);
		r.shutdown();
	}
	public void connect() {
		if(session == null) {
			Cluster.Builder clusterBuilder = Cluster.builder()
			    .addContactPoints(
			        "23.20.121.214", "34.195.86.103", "35.172.77.138" // AWS_VPC_US_EAST_1 (Amazon Web Services (VPC))
			    )
			    .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("AWS_VPC_US_EAST_1").build()) // your local data centre
			    .withPort(9042)
			    .withAuthProvider(new PlainTextAuthProvider("iccassandra", "a832a593f4bd5098d30e77776c668c77"));
			cluster = clusterBuilder.build();
			session = cluster.connect();
		}
		//return session;
	}

	
	
	public void execute(String cqlFile) {
		System.out.println("Running file " + cqlFile);
		String readFileIntoString = FileUtils.readFileIntoString(cqlFile);
		String[] commands = readFileIntoString.split(";");
		for (String command : commands){
			
			String cql = command.trim();
			
			if (cql.isEmpty()){
				continue;
			}
			
			if (cql.toLowerCase().startsWith("drop")){
				this.runAllowFail(cql);
			}else{
				this.run(cql);
			}			
		}
	}
	
	

	void runAllowFail(String cql) {
		try {
			run(cql);
		} catch (Exception e) {
			System.out.println("Ignoring exception - " + e.getMessage());
		}
	}

	void run(String cql){
		System.out.println("Query : " + cql);
		long startTime = System.currentTimeMillis();
		if(startsWithIgnoreCase(cql, "select")){
			ResultSet rs = session.execute(cql);
			long t = System.currentTimeMillis();
			System.out.println(" Execute timetaken="+Utils.formatTime(t-startTime));
			int rowCount = 0;
			for(Row row : rs) {
				rowCount++;
			}
			System.out.println(" Fetch noRows="+rowCount+" timetaken="+Utils.formatTime(System.currentTimeMillis()-t));
		}else {
			session.execute(cql);
		}
		long endTime = System.currentTimeMillis();
		System.out.println(" Total timetaken="+Utils.formatTime(endTime-startTime));
	}
	
    public static boolean startsWithIgnoreCase(String str, String prefix)
    {
        return str.regionMatches(true, 0, prefix, 0, prefix.length());
    }
    
	void shutdown() {
		session.close();
		cluster.close();
	}
}
