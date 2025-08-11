-- Real Estate UDT Definitions
-- Execute these first before inserting sample data

USE real_estate;

-- Address Information
CREATE TYPE IF NOT EXISTS address_details (
    street_number TEXT,
    street_name TEXT,
    unit_number TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county TEXT,
    country TEXT,
    latitude DECIMAL,
    longitude DECIMAL,
    timezone TEXT,
    neighborhood TEXT,
    subdivision TEXT
);

-- Property Specifications
CREATE TYPE IF NOT EXISTS property_specifications (
    property_type TEXT,
    property_subtype TEXT,
    square_feet INT,
    lot_size_sqft INT,
    bedrooms INT,
    bathrooms DECIMAL,
    half_baths INT,
    stories INT,
    year_built INT,
    year_renovated INT,
    garage_spaces INT,
    parking_spaces INT,
    basement_type TEXT,
    foundation_type TEXT,
    roof_type TEXT,
    exterior_material TEXT,
    heating_type TEXT,
    cooling_type TEXT,
    flooring_types LIST<TEXT>
);

-- Property Amenities
CREATE TYPE IF NOT EXISTS property_amenities (
    pool BOOLEAN,
    pool_type TEXT,
    spa_hot_tub BOOLEAN,
    fireplace BOOLEAN,
    fireplace_count INT,
    deck BOOLEAN,
    patio BOOLEAN,
    balcony BOOLEAN,
    fence BOOLEAN,
    fence_type TEXT,
    security_system BOOLEAN,
    alarm_system BOOLEAN,
    sprinkler_system BOOLEAN,
    central_vacuum BOOLEAN,
    intercom_system BOOLEAN,
    elevator BOOLEAN,
    wheelchair_accessible BOOLEAN,
    solar_panels BOOLEAN,
    energy_efficient_appliances BOOLEAN,
    smart_home_features LIST<TEXT>,
    outdoor_features LIST<TEXT>,
    interior_features LIST<TEXT>
);

-- Financial Details
CREATE TYPE IF NOT EXISTS financial_details (
    listing_price DECIMAL,
    original_list_price DECIMAL,
    price_per_sqft DECIMAL,
    last_sold_price DECIMAL,
    last_sold_date DATE,
    assessed_value DECIMAL,
    assessment_year INT,
    annual_property_taxes DECIMAL,
    monthly_hoa_fees DECIMAL,
    hoa_name TEXT,
    special_assessments DECIMAL,
    homeowners_insurance_estimate DECIMAL,
    utilities_included LIST<TEXT>,
    financing_options LIST<TEXT>
);

-- Location Quality
CREATE TYPE IF NOT EXISTS location_quality (
    school_district TEXT,
    elementary_school TEXT,
    middle_school TEXT,
    high_school TEXT,
    school_ratings MAP<TEXT, INT>,
    crime_index INT,
    walkability_score INT,
    transit_score INT,
    bike_score INT,
    noise_level TEXT,
    air_quality_index INT,
    flood_zone TEXT,
    earthquake_zone TEXT,
    hurricane_zone TEXT,
    nearby_amenities MAP<TEXT, DECIMAL>,
    commute_times MAP<TEXT, INT>,
    walkable_destinations LIST<TEXT>
);



-- Market Intelligence
CREATE TYPE IF NOT EXISTS market_intelligence (
    days_on_market INT,
    listing_views INT,
    showing_count INT,
    offer_count INT,
    market_temperature TEXT,
    price_trend_30_days DECIMAL,
    price_trend_90_days DECIMAL,
    price_trend_1_year DECIMAL,
    comparable_sales_count INT,
    inventory_level TEXT,
    absorption_rate DECIMAL,
    median_dom_area INT,
    price_per_sqft_area DECIMAL,
    market_velocity TEXT
);

-- Listing Details
CREATE TYPE IF NOT EXISTS listing_details (
    listing_agent_name TEXT,
    listing_agent_phone TEXT,
    listing_agent_email TEXT,
    listing_brokerage TEXT,
    listing_date DATE,
    listing_status TEXT,
    listing_type TEXT,
    showing_instructions TEXT,
    commission_rate DECIMAL,
    buyer_agent_commission DECIMAL,
    listing_remarks TEXT,
    private_remarks TEXT,
    virtual_tour_url TEXT,
    video_tour_url TEXT,
    floor_plan_url TEXT
);
