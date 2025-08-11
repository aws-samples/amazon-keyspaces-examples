-- Sample Data: Properties by Location Table
USE real_estate;

-- Insert properties into location-based table for efficient geographic queries

-- Seattle Capitol Hill - Luxury Property
INSERT INTO properties_by_location (
    zip_code,
    property_type,
    price_range,
    property_id,
    address,
    specifications,
    financial,
    location,
    listing,
    market
) VALUES (
    '98102',
    'single_family',
    '2m-3m',
    11111111-1111-1111-1111-111111111111,
    {
        street_number: '1234',
        street_name: 'Bellevue Avenue',
        city: 'Seattle',
        state: 'WA',
        zip_code: '98102',
        county: 'King County',
        country: 'USA',
        latitude: 47.6205,
        longitude: -122.3212,
        neighborhood: 'Capitol Hill'
    },
    {
        property_type: 'single_family',
        square_feet: 4500,
        bedrooms: 5,
        bathrooms: 4.5,
        year_built: 2018,
        garage_spaces: 3
    },
    {
        listing_price: 2850000,
        price_per_sqft: 633.33,
        annual_property_taxes: 28500
    },
    {
        school_district: 'Seattle Public Schools',
        crime_index: 25,
        walkability_score: 88,
        transit_score: 75
    },
    {
        listing_agent_name: 'Sarah Johnson',
        listing_status: 'active',
        listing_date: '2024-06-01'
    },
    {
        days_on_market: 45,
        market_temperature: 'balanced',
        price_trend_1_year: 5.2
    }
);

-- Seattle Downtown - Condo
INSERT INTO properties_by_location (
    zip_code,
    property_type,
    price_range,
    property_id,
    address,
    specifications,
    financial,
    location,
    listing,
    market
) VALUES (
    '98101',
    'condo',
    '1m-1.5m',
    22222222-2222-2222-2222-222222222222,
    {
        street_number: '500',
        street_name: 'Pine Street',
        unit_number: '2801',
        city: 'Seattle',
        state: 'WA',
        zip_code: '98101',
        county: 'King County',
        country: 'USA',
        latitude: 47.6097,
        longitude: -122.3331,
        neighborhood: 'Downtown'
    },
    {
        property_type: 'condo',
        square_feet: 1850,
        bedrooms: 2,
        bathrooms: 2.0,
        year_built: 2020,
        garage_spaces: 1
    },
    {
        listing_price: 1250000,
        price_per_sqft: 675.68,
        monthly_hoa_fees: 850,
        annual_property_taxes: 12500
    },
    {
        school_district: 'Seattle Public Schools',
        crime_index: 35,
        walkability_score: 98,
        transit_score: 95
    },
    {
        listing_agent_name: 'Michael Chen',
        listing_status: 'active',
        listing_date: '2024-07-03'
    },
    {
        days_on_market: 12,
        market_temperature: 'hot',
        price_trend_1_year: 8.1
    }
);

-- Bellevue - Family Home
INSERT INTO properties_by_location (
    zip_code,
    property_type,
    price_range,
    property_id,
    address,
    specifications,
    financial,
    location,
    listing,
    market
) VALUES (
    '98008',
    'single_family',
    '750k-1m',
    33333333-3333-3333-3333-333333333333,
    {
        street_number: '15678',
        street_name: 'Maple Ridge Drive',
        city: 'Bellevue',
        state: 'WA',
        zip_code: '98008',
        county: 'King County',
        country: 'USA',
        latitude: 47.6101,
        longitude: -122.2015,
        neighborhood: 'Somerset'
    },
    {
        property_type: 'single_family',
        square_feet: 3200,
        bedrooms: 4,
        bathrooms: 3.0,
        year_built: 1995,
        garage_spaces: 2
    },
    {
        listing_price: 875000,
        price_per_sqft: 273.44,
        annual_property_taxes: 8750
    },
    {
        school_district: 'Bellevue School District',
        crime_index: 15,
        walkability_score: 45,
        transit_score: 35
    },
    {
        listing_agent_name: 'Jennifer Martinez',
        listing_status: 'active',
        listing_date: '2024-06-18'
    },
    {
        days_on_market: 28,
        market_temperature: 'balanced',
        price_trend_1_year: 6.3
    }
);

-- Seattle Columbia City - Investment Property
INSERT INTO properties_by_location (
    zip_code,
    property_type,
    price_range,
    property_id,
    address,
    specifications,
    financial,
    location,
    listing,
    market
) VALUES (
    '98144',
    'multi_family',
    '500k-750k',
    44444444-4444-4444-4444-444444444444,
    {
        street_number: '2847',
        street_name: 'Rainier Avenue',
        city: 'Seattle',
        state: 'WA',
        zip_code: '98144',
        county: 'King County',
        country: 'USA',
        latitude: 47.5868,
        longitude: -122.2968,
        neighborhood: 'Columbia City'
    },
    {
        property_type: 'multi_family',
        square_feet: 2400,
        bedrooms: 4,
        bathrooms: 2.0,
        year_built: 1925,
        garage_spaces: 0
    },
    {
        listing_price: 695000,
        price_per_sqft: 289.58,
        annual_property_taxes: 6950
    },
    {
        school_district: 'Seattle Public Schools',
        crime_index: 45,
        walkability_score: 78,
        transit_score: 85
    },
    {
        listing_agent_name: 'Robert Kim',
        listing_status: 'active',
        listing_date: '2024-05-08'
    },
    {
        days_on_market: 67,
        market_temperature: 'cool',
        price_trend_1_year: 4.8
    }
);
