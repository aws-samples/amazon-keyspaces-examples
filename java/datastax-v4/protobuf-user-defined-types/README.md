# Amazon Keyspaces Protocol Buffers

This example demonstrates how to use Protocol Buffers (Protobuf) with Amazon Keyspaces to provide an alternative to
Cassandra User Defined Types (UDTs). Protocol Buffers is a free and open-source cross-platform data format which is 
used to serialize structured data. By storing the data as CQL BLOBs, architects can refactor UDTS while still 
preserving structured data across applications and programming languages.

## What is a user defined type in Apache Cassandra

Apache Cassandra supports User Defined Types (UDT), which enable users to create complex structures for storage within 
a single column/cell. These structures can incorporate a variety of Cassandra data types, including collections. 
UDTs can then be incorporated into tables, as well as nested within collections or other UDTs.

The following example of a UDT represents a user profile made of simple types, collections, and a nested UDT. A user 
profile can contain multiple phone numbers. PhoneNumber is also a UDT containing multiple fields representing the different 
components of a phone number.

```
CREATE TYPE aws.phone_number (
       country_code text,
       area_code text,
       prefix text,
       line text
 );
CREATE TYPE aws.user_profile (
   id UUID,
   name text,
   age int,
   emails list<text>
   numbers map<text, frozen<phone_number>>
);
CREATE TABLE IF NOT EXISTS aws.protobuf_test (
     key text PRIMARY KEY, 
     value text, 
     data user_profile);
```

## Translate to Protobuf
Protocol Buffers are analogous to user-defined types in terms of their structure and capacity, yet they provide various advantages.
For instance, default values are incorporated into the schema instead of being stored for each entry, which can decrease 
the total size. Furthermore, the Protobuf message definition can be extended independently by different applications while 
still maintaining backward compatibility.

Creating a .proto file begins the process, in which message types are defined. Each field is identified by a unique 
field number, and is composed of a data type, name, and, optionally, a custom default value. As field numbers must 
remain consistent in order to ensure backwards compatibility of the message binary format.  You can also use the 
optional tag to distinguish unset values.
```
message PhoneNumber {
    string countryCode = 1 [default = 1];
    string areaCode = 2;
    string prefix = 3;
    string line = 4;
}
message Uuid {
  bytes value = 1;
}
message UserProfile {
  Uuid id = 1;
  string name = 2;
  uint32 age = 3;
  repeated string emails = 4;
  map<string, PhoneNumber> numbers = 5;
}

```

## Store protobuf in Amazon Keyspaces
Storing a protobuf value in Amazon Keyspaces requires the use of a CQL BLOB type. In the following example we use a simple key-value 
model that contains a BLOB field to demonstrate this. 

```
CREATE TABLE IF NOT EXISTS aws.protobuf_test (
     key text PRIMARY KEY, 
     value text, 
     data blob);
```

 ## Building and Generating Protobuf class

 In order to build the example code, you will need Maven version 3.6.3 or higher, which you can obtain from
 https://maven.apache.org/. Once you have Maven installed, you can build the project which will generate the proto code 
 from the .proto file. 

 ``` shell
 mvn clean compile
 ```


 ## Running the example

From an IDE you can execute ProtobufExampleMain to connect to Keyspaces, create a new table, insert a row containing a 
protobuf message, and then read it out with strong consistency. 