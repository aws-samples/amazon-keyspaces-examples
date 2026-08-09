# Start with a base image containing Java runtime
FROM --platform=linux/amd64 openjdk:17-jdk-alpine

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Add a volume pointing to /tmp
VOLUME /tmp

# The application's jar file
ARG JAR_FILE=target/mykeyspacessigv4springbootapp-1.0-SNAPSHOT.jar

# Add the application's jar to the container
COPY ${JAR_FILE} app.jar

# Add the truststore to the container
COPY cassandra_truststore.jks /usr/app/cassandra_truststore.jks

# Add the application's properties to the container
COPY src/main/resources/application.conf /usr/app/application.conf

# Add the application's logback configuration to the container
COPY src/main/resources/logback.xml /usr/app/logback.xml

# Run the jar file
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]

