package com.example.model;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends CassandraRepository<User, String> {
    @Query("SELECT username, fname, lname FROM user WHERE username = ?0")
    User findByUsername(final String username);
}
