-- Sample Queries for Real Estate Data
-- These queries demonstrate how to use the UDT-based schema

USE real_estate;

-- =============================================
-- PROPERTY SEARCH QUERIES
-- =============================================

-- Query 1: List all of the properties (only use for small tables)

select * from properties;

-- Query 2: Find luxury properties by MLS number
-- Shows how to access nested UDT fields
SELECT property_id,
       address.street_number , address.street_name,
       address.city,
       specifications.bedrooms,
       specifications.bathrooms,
       specifications.square_feet,
       financial.listing_price,
       financial.price_per_sqft,
       financial.listing_price,
       amenities.pool,
       amenities.smart_home_features,
       location.walkability_score,
       listing.listing_agent_name
FROM properties
WHERE  mls_number = 'MLS2024004'
ALLOW FILTERING
;

-- Query 3: Properties by location and price range
-- Optimized query using the location-based table

SELECT property_id,
       address.street_name,
       address.city,
       specifications.bedrooms,
       specifications.bathrooms,
       financial.listing_price,
       location.school_district,
       listing.listing_status
FROM properties_by_location 
WHERE zip_code = '98101' 
AND property_type = 'condo'
AND price_range IN ('1m-1.5m', '1.5m-2m');


-- Query 4: Compare similar properties in same area

SELECT property_id,
       address.neighborhood,
       specifications.square_feet,
       specifications.bedrooms,
       specifications.year_built,
       financial.listing_price,
       financial.price_per_sqft,
       market.days_on_market
FROM properties_by_location 
WHERE zip_code = '98102'
AND property_type = 'single_family'
AND price_range = '2m-3m';



-- =============================================
-- SAMPLE DATA VERIFICATION QUERIES
-- =============================================

-- Verify UDT data structure

SELECT property_id,
       address,
       specifications,
       amenities.smart_home_features,
       financial.listing_price,
       location.school_ratings
FROM properties 
LIMIT 2;


