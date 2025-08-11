package com.example.demo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.model.User;
import com.example.model.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;


@SpringBootApplication
@EnableCassandraRepositories(basePackages="com.example.model")
public class DemoApplication implements CommandLineRunner {

	@Autowired(required = true)
	private UserRepository sampleUserRepository;

	@Autowired
	private CassandraOperations cassandraTemplate;

	public static void main(String[] args) {

		SpringApplication.run(DemoApplication.class, args);

	}

	@Override
	public void run(String... args) throws Exception {

		CqlTemplate cqlTemplate = (CqlTemplate) cassandraTemplate.getCqlOperations();

		CqlSession session = cqlTemplate.getSession();

		int count = session.execute ("SELECT * FROM system.peers").all().size();

		System.out.println("Number of hosts: "+  count);

		String userName = "aws-user";

		User userIn = new User();
		userIn.setUsername(userName);
		userIn.setFname("emma");
		userIn.setLname("brie");

		sampleUserRepository.insert(userIn);

		User userOut = sampleUserRepository.findByUsername(userName);

		System.out.println("Primary Key: " + userOut.getUsername());

	}



}
