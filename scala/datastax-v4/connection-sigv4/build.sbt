name := "scalaexamplesv4"
version := "1.0"
scalaVersion := "2.13.10"
libraryDependencies += "software.aws.mcs" % "aws-sigv4-auth-cassandra-java-driver-plugin" % "4.0.8"
libraryDependencies += "com.datastax.oss" % "java-driver-core" % "4.15.0"
trapExit := false
