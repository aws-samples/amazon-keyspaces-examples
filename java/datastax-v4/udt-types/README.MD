# User-defined types (UDTs) in Amazon Keyspaces

Amazon Keyspaces (for Apache Cassandra) allows the use of user-defined types (UDTs) to optimize data organization and enhance data modeling capabilities.

A user-defined type (UDT) is a grouping of fields and data types that you can use to define a single column in Amazon Keyspaces. 
Valid data types for UDTs are all supported Cassandra data types, including collections and other UDTs that you've already created in the same keyspace. 

For more information about supported Keyspaces data types, see [Cassandra data type support](https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html#cassandra-data-type) .


# Amazon Keyspaces Real Estate Schema Example

This example demonstrates a comprehensive Real Estate data model using Amazon Keyspaces with User Defined Types (UDTs). The schema showcases how to structure complex real estate data including property details, market analytics, and location-based queries.


## Schema Overview
The real estate schema consists of three main tables and seven UDTs that model comprehensive property information:

### User Defined Types (UDTs)

- **address_details**: Complete address information including coordinates and neighborhood data
- **property_specifications**: Physical property details (size, rooms, construction details)
- **property_amenities**: Features and amenities (pool, security, smart home features)
- **financial_details**: Pricing, taxes, HOA fees, and financing information
- **location_quality**: School districts, walkability scores, and area ratings
- **market_intelligence**: Market trends, days on market, and pricing analytics
- **listing_details**: Agent information, listing status, and marketing materials

### Tables

1. **properties**: Main property table with complete property information
2. **properties_by_location**: Optimized for geographic and price range queries
3. **market_analytics**: Time-series market data for trend analysis

## Sample Data

The schema includes sample data for the Seattle/Bellevue area:

- **Luxury Properties**: High-end homes and condos ($1M+)
- **Mid-Range Properties**: Family homes and investment properties ($500K-$1M)
- **Market Analytics**: Historical market data with trends and statistics
- **Location Data**: Properties organized by ZIP code and property type

## Quick Start

### Prerequisites

- Amazon Keyspaces access or local Cassandra installation
- cqlsh or cqlsh-expansion for Amazon Keyspaces

### Setup

1. **Execute the setup script:**
   ```bash
   cd real_eastate_schema_sample
   chmod +x execute_real_estate_schema.sh
   ./execute_real_estate_schema.sh
   ```

2. **Manual setup (alternative):**
   ```bash
   # Create keyspace
   cqlsh -e "CREATE KEYSPACE IF NOT EXISTS real_estate WITH REPLICATION = {'class': 'SingleRegionStrategy'};"
   
   # Execute files in order
   cqlsh -f 01_real_estate_udts.cql
   cqlsh -f 02_properties_table.cql
   cqlsh -f 03_sample_luxury_properties.cql
   cqlsh -f 04_sample_midrange_properties.cql
   cqlsh -f 05_sample_properties_by_location.cql
   cqlsh -f 06_sample_market_analytics.cql
   ```


## Sample Queries

The schema supports various query patterns:

### Location-Based Queries
```cql
-- Properties by ZIP code and type
SELECT property_id, address.street_name, financial.listing_price
FROM properties_by_location 
WHERE zip_code = '98101' 
AND property_type = 'condo';
```

### Market Analytics
```cql
-- Market trends over time
SELECT area_code, date, avg_price, median_price, 
       market_stats.market_temperature
FROM market_analytics 
WHERE area_code = '98101' 
AND property_type = 'condo';
```

## Schema Features

### Complex Data Modeling
- **Nested UDTs**: Rich data structures for comprehensive property information
- **Collections**: Lists and maps for amenities, school ratings, and features
- **Flexible Schema**: Easy to extend with additional property attributes

### Query Optimization
- **Partition Keys**: Efficient data distribution by location and property type
- **Clustering Keys**: Ordered data for range queries and time-series analysis

### Real Estate Use Cases
- **Property Listings**: Complete MLS-style property information
- **Market Analysis**: Historical trends and comparative market analysis
- **Location Intelligence**: School districts, walkability, and neighborhood data
- **Investment Analysis**: Financial metrics and market performance

## File Structure

```
real_eastate_schema_sample/
├── 01_real_estate_udts.cql          # UDT definitions
├── 02_properties_table.cql          # Table schemas
├── 03_sample_luxury_properties.cql  # High-end property samples
├── 04_sample_midrange_properties.cql # Mid-range property samples
├── 05_sample_properties_by_location.cql # Location-based data
├── 06_sample_market_analytics.cql   # Market trend data
├── 07_sample_queries.cql            # Example queries
└── execute_real_estate_schema.sh    # Setup script
```

## Data Model Benefits

### Structured Data
- **Type Safety**: UDTs provide schema validation
- **Data Integrity**: Consistent structure across all properties
- **Rich Metadata**: Comprehensive property and market information

### Flexibility
- **Extensible**: Easy to add new UDT fields
- **Backward Compatible**: Schema evolution without breaking changes
- **Multi-Use**: Supports various real estate applications

---
## Amazon Keyspaces Considerations
When using with Amazon Keyspaces:
- Use point-in-time recovery for data protection
- Consider on-demand billing for variable workloads
- Implement proper IAM policies for data access

### Next Steps

1. **Extend the Schema**: Add more UDTs for additional property types
2. **Implement Applications**: Build real estate applications using this schema
3. **Monitor Performance**: Use CloudWatch metrics to optimize queries
