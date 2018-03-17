package org.am.cdo.conn.test;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

public class TestInstaCluster {

	public static void main(String[] args) {
		final Cluster.Builder clusterBuilder = Cluster.builder()
			    .addContactPoints(
			        "23.20.121.214", "34.195.86.103", "35.172.77.138" // AWS_VPC_US_EAST_1 (Amazon Web Services (VPC))
			    )
			    .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("AWS_VPC_US_EAST_1").build()) // your local data centre
			    .withPort(9042)
			    .withAuthProvider(new PlainTextAuthProvider("iccassandra", "a832a593f4bd5098d30e77776c668c77"));

			try (final Cluster cluster = clusterBuilder.build()) {
			    final Metadata metadata = cluster.getMetadata();
			    System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

			    for (final Host host: metadata.getAllHosts()) {
			        System.out.printf("Datacenter: %s; Host: %s; Rack: %s; isUp: %s; listen addr:%s \n", host.getDatacenter(), host.getAddress(), host.getRack(), host.isUp(), host.getListenAddress());
			    }
			    
			}
	}
}
