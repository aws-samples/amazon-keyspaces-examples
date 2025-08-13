#!/bin/bash

# Real Estate Schema Setup Script
# This script executes all CQL files in the correct order

echo "Setting up Real Estate Schema with Sample Data..."
echo "================================================"

# Check if cqlsh is available
if ! command -v cqlsh &> /dev/null; then
    echo "Error: cqlsh is not installed or not in PATH"
    echo "Please install Cassandra or Amazon Keyspaces cqlsh-expansion"
    echo ""
    echo "For local Cassandra (macOS): brew install cassandra"
    echo "For Amazon Keyspaces: pip install cqlsh-expansion"
    exit 1
fi

# Create keyspace first
echo "Step 1: Creating keyspace..."
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS real_estate WITH REPLICATION = {'class': 'SingleRegionStrategy'};"

if [ $? -eq 0 ]; then
    echo "✓ Keyspace created successfully"
else
    echo "✗ Failed to create keyspace"
    exit 1
fi

# Execute CQL files in order
files=(
    "01_real_estate_udts.cql"
    "02_properties_table.cql"
    "03_sample_luxury_properties.cql"
    "04_sample_midrange_properties.cql"
    "05_sample_properties_by_location.cql"
    "06_sample_market_analytics.cql"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "Step: Executing $file..."
        cqlsh -f "$file"
        
        if [ $? -eq 0 ]; then
            echo "✓ $file executed successfully"
        else
            echo "✗ Failed to execute $file"
            exit 1
        fi
    else
        echo "✗ File $file not found"
        exit 1
    fi
done

echo ""
echo "================================================"
echo "✓ Real Estate Schema Setup Complete!"
echo ""
echo "Sample data has been loaded into the following tables:"
echo "- properties (4 sample properties)"
echo "- properties_by_location (4 properties for location queries)"
echo "- market_analytics (7 market data points)"
echo ""
echo "You can now run the sample queries in 07_sample_queries.cql"
echo ""

