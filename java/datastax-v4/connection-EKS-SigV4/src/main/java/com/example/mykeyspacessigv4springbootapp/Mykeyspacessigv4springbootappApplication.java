package com.example.mykeyspacessigv4springbootapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import javax.annotation.PreDestroy;
import java.io.File;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import javax.net.ssl.SSLContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import software.aws.mcs.auth.SigV4AuthProvider;

@SpringBootApplication
public class Mykeyspacessigv4springbootappApplication {

	private CqlSession session;

	public static void main(String[] args) {
		SpringApplication.run(Mykeyspacessigv4springbootappApplication.class, args);
	}

	@Bean
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		PropertySourcesPlaceholderConfigurer properties = new PropertySourcesPlaceholderConfigurer();
		properties.setLocation(new ClassPathResource("application.conf"));
		properties.setIgnoreResourceNotFound(false);
		properties.setIgnoreUnresolvablePlaceholders(false);
		return properties;
	}

	@Bean
	public CqlSession cqlSession() throws NoSuchAlgorithmException {
		String cassandraHost = System.getenv("CASSANDRA_HOST");
		String cassandraDC = System.getenv("CASSANDRA_DC");

		var builder = CqlSession.builder()
				.addContactPoint(new InetSocketAddress(cassandraHost.split(":")[0], Integer.parseInt(cassandraHost.split(":")[1])))
				.withAuthProvider(new SigV4AuthProvider(cassandraDC))
				.withLocalDatacenter(cassandraDC)
				.withSslContext(SSLContext.getDefault())
				.withConfigLoader(DriverConfigLoader.fromClasspath("application.conf"));

		session = builder.build();

		// Insert statement
		String query = "INSERT INTO aws.user (username,fname,last_update_date,lname) VALUES ('test','random',dateOf(now()),'k')";
		SimpleStatement statement = SimpleStatement.newInstance(query).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		session.execute(statement);
		System.out.println("Insert operation was successful. The following row was added:");
		System.out.println("Username: test");
		System.out.println("First Name: random");
		System.out.println("Last Update Date: " + LocalDate.now());
		System.out.println("Last Name: k");

		return session;
	}

	@PreDestroy
	public void preDestroy() {
		if (session != null) {
			session.close();
		}
	}
}
