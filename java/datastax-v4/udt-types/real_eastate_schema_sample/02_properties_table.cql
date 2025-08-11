-- Main Properties Table
USE real_estate;

CREATE TABLE IF NOT EXISTS properties (
    property_id UUID PRIMARY KEY,
    mls_number TEXT,
    address frozen<address_details>,
    specifications frozen<property_specifications>,
    amenities frozen<property_amenities>,
    financial frozen<financial_details>,
    location frozen<location_quality>,
    market frozen<market_intelligence>,
    listing frozen<listing_details>,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    data_source TEXT,
    data_quality_score INT
);

-- Properties by Location Table
CREATE TABLE IF NOT EXISTS properties_by_location (
    zip_code TEXT,
    property_type TEXT,
    price_range TEXT,
    property_id UUID,
    address frozen<address_details>,
    specifications frozen<property_specifications>,
    financial frozen<financial_details>,
    location frozen<location_quality>,
    listing frozen<listing_details>,
    market frozen<market_intelligence>,
    PRIMARY KEY ((zip_code, property_type), price_range, property_id)
) WITH CLUSTERING ORDER BY (price_range ASC, property_id ASC);

-- Market Analytics Table
CREATE TABLE IF NOT EXISTS market_analytics (
    area_code TEXT,
    date DATE,
    property_type TEXT,
    avg_price DECIMAL,
    median_price DECIMAL,
    avg_price_per_sqft DECIMAL,
    total_listings INT,
    new_listings INT,
    closed_sales INT,
    pending_sales INT,
    avg_days_on_market INT,
    inventory_months DECIMAL,
    market_stats frozen<market_intelligence>,
    PRIMARY KEY ((area_code, property_type), date)
) WITH CLUSTERING ORDER BY (date DESC);
