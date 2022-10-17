package com.example.model;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends CassandraRepository<User, String> {
    User findByUsername(final String username);
}
