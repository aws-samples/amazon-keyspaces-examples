-- Sample Data: Luxury Properties
USE real_estate;

-- Luxury Single Family Home in Seattle
INSERT INTO properties (
    property_id,
    mls_number,
    address,
    specifications,
    amenities,
    financial,
    location,
    market,
    listing,
    created_at,
    updated_at,
    data_source,
    data_quality_score
) VALUES (
    11111111-1111-1111-1111-111111111111,
    'MLS2024001',
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
        timezone: 'PST',
        neighborhood: 'Capitol Hill',
        subdivision: 'Bellevue Heights'
    },
    {
        property_type: 'single_family',
        property_subtype: 'contemporary',
        square_feet: 4500,
        lot_size_sqft: 8000,
        bedrooms: 5,
        bathrooms: 4.5,
        half_baths: 1,
        stories: 3,
        year_built: 2018,
        year_renovated: 2023,
        garage_spaces: 3,
        parking_spaces: 4,
        basement_type: 'full',
        foundation_type: 'concrete',
        roof_type: 'composition',
        exterior_material: 'brick_stone',
        heating_type: 'forced_air',
        cooling_type: 'central_air',
        flooring_types: ['hardwood', 'tile', 'carpet']
    },
    {
        pool: true,
        pool_type: 'inground',
        spa_hot_tub: true,
        fireplace: true,
        fireplace_count: 2,
        deck: true,
        patio: true,
        balcony: false,
        fence: true,
        fence_type: 'privacy',
        security_system: true,
        alarm_system: true,
        sprinkler_system: true,
        central_vacuum: true,
        intercom_system: false,
        elevator: false,
        wheelchair_accessible: false,
        solar_panels: true,
        energy_efficient_appliances: true,
        smart_home_features: ['smart_thermostat', 'smart_lighting', 'smart_security', 'voice_control'],
        outdoor_features: ['outdoor_kitchen', 'fire_pit', 'landscaped_garden'],
        interior_features: ['wine_cellar', 'home_theater', 'office', 'gym']
    },
    {
        listing_price: 2850000,
        original_list_price: 2950000,
        price_per_sqft: 633.33,
        last_sold_price: 2200000,
        last_sold_date: '2018-06-15',
        assessed_value: 2650000,
        assessment_year: 2024,
        annual_property_taxes: 28500,
        monthly_hoa_fees: 0,
        hoa_name: null,
        special_assessments: 0,
        homeowners_insurance_estimate: 4200,
        utilities_included: [],
        financing_options: ['conventional', 'jumbo', 'cash']
    },
    {
        school_district: 'Seattle Public Schools',
        elementary_school: 'Stevens Elementary',
        middle_school: 'Meany Middle School',
        high_school: 'Garfield High School',
        school_ratings: {'Stevens Elementary': 8, 'Meany Middle School': 7, 'Garfield High School': 9},
        crime_index: 25,
        walkability_score: 88,
        transit_score: 75,
        bike_score: 82,
        noise_level: 'moderate',
        air_quality_index: 45,
        flood_zone: 'X',
        earthquake_zone: 'moderate',
        hurricane_zone: 'none',
        nearby_amenities: {'grocery_store': 0.3, 'restaurant': 0.1, 'park': 0.2, 'hospital': 1.2},
        commute_times: {'downtown_seattle': 15, 'bellevue': 25, 'airport': 35},
        walkable_destinations: ['coffee_shops', 'restaurants', 'retail', 'parks']
    },
    {
        days_on_market: 45,
        listing_views: 2847,
        showing_count: 23,
        offer_count: 3,
        market_temperature: 'balanced',
        price_trend_30_days: -2.1,
        price_trend_90_days: 1.8,
        price_trend_1_year: 5.2,
        comparable_sales_count: 8,
        inventory_level: 'normal',
        absorption_rate: 3.2,
        median_dom_area: 52,
        price_per_sqft_area: 625.50,
        market_velocity: 'normal'
    },
    {
        listing_agent_name: 'Sarah Johnson',
        listing_agent_phone: '206-555-0123',
        listing_agent_email: 'sarah.johnson@luxuryrealty.com',
        listing_brokerage: 'Luxury Realty Group',
        listing_date: '2024-06-01',
        listing_status: 'active',
        listing_type: 'exclusive',
        showing_instructions: 'Call listing agent 24 hours in advance',
        commission_rate: 5.5,
        buyer_agent_commission: 2.75,
        listing_remarks: 'Stunning contemporary home with panoramic city views. Recently renovated with high-end finishes throughout.',
        private_remarks: 'Motivated seller, will consider reasonable offers',
        virtual_tour_url: 'https://virtualtour.com/property123',
        video_tour_url: 'https://youtube.com/watch?v=property123',
        floor_plan_url: 'https://floorplans.com/property123.pdf'
    },
    '2024-06-01 10:00:00',
    '2024-07-15 14:30:00',
    'mls',
    95
);

-- Modern Condo in Downtown Seattle
INSERT INTO properties (
    property_id,
    mls_number,
    address,
    specifications,
    amenities,
    financial,
    location,
    market,
    listing,
    created_at,
    updated_at,
    data_source,
    data_quality_score
) VALUES (
    22222222-2222-2222-2222-222222222222,
    'MLS2024002',
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
        timezone: 'PST',
        neighborhood: 'Downtown',
        subdivision: 'Metropolitan Tower'
    },
    {
        property_type: 'condo',
        property_subtype: 'high_rise',
        square_feet: 1850,
        lot_size_sqft: 0,
        bedrooms: 2,
        bathrooms: 2.0,
        half_baths: 0,
        stories: 1,
        year_built: 2020,
        year_renovated: null,
        garage_spaces: 1,
        parking_spaces: 1,
        basement_type: 'none',
        foundation_type: 'concrete',
        roof_type: 'flat',
        exterior_material: 'glass_steel',
        heating_type: 'radiant',
        cooling_type: 'central_air',
        flooring_types: ['hardwood', 'tile']
    },
    {
        pool: false,
        pool_type: null,
        spa_hot_tub: false,
        fireplace: true,
        fireplace_count: 1,
        deck: false,
        patio: false,
        balcony: true,
        fence: false,
        fence_type: null,
        security_system: true,
        alarm_system: true,
        sprinkler_system: true,
        central_vacuum: false,
        intercom_system: true,
        elevator: true,
        wheelchair_accessible: true,
        solar_panels: false,
        energy_efficient_appliances: true,
        smart_home_features: ['smart_thermostat', 'smart_lighting', 'keyless_entry'],
        outdoor_features: ['balcony_city_view'],
        interior_features: ['floor_to_ceiling_windows', 'granite_counters', 'stainless_appliances']
    },
    {
        listing_price: 1250000,
        original_list_price: 1250000,
        price_per_sqft: 675.68,
        last_sold_price: null,
        last_sold_date: null,
        assessed_value: 1180000,
        assessment_year: 2024,
        annual_property_taxes: 12500,
        monthly_hoa_fees: 850,
        hoa_name: 'Metropolitan Tower HOA',
        special_assessments: 0,
        homeowners_insurance_estimate: 1800,
        utilities_included: ['water', 'sewer', 'garbage'],
        financing_options: ['conventional', 'fha', 'va']
    },
    {
        school_district: 'Seattle Public Schools',
        elementary_school: 'Lowell Elementary',
        middle_school: 'Washington Middle School',
        high_school: 'Cleveland High School',
        school_ratings: {'Lowell Elementary': 6, 'Washington Middle School': 5, 'Cleveland High School': 6},
        crime_index: 35,
        walkability_score: 98,
        transit_score: 95,
        bike_score: 75,
        noise_level: 'busy',
        air_quality_index: 52,
        flood_zone: 'X',
        earthquake_zone: 'moderate',
        hurricane_zone: 'none',
        nearby_amenities: {'grocery_store': 0.1, 'restaurant': 0.05, 'park': 0.3, 'hospital': 0.8},
        commute_times: {'downtown_seattle': 5, 'bellevue': 20, 'airport': 30},
        walkable_destinations: ['restaurants', 'shopping', 'entertainment', 'transit']
    },
    {
        days_on_market: 12,
        listing_views: 1456,
        showing_count: 18,
        offer_count: 2,
        market_temperature: 'hot',
        price_trend_30_days: 3.2,
        price_trend_90_days: 4.8,
        price_trend_1_year: 8.1,
        comparable_sales_count: 15,
        inventory_level: 'low',
        absorption_rate: 1.8,
        median_dom_area: 28,
        price_per_sqft_area: 685.25,
        market_velocity: 'fast'
    },
    {
        listing_agent_name: 'Michael Chen',
        listing_agent_phone: '206-555-0456',
        listing_agent_email: 'michael.chen@urbanrealty.com',
        listing_brokerage: 'Urban Realty Partners',
        listing_date: '2024-07-03',
        listing_status: 'active',
        listing_type: 'exclusive',
        showing_instructions: 'Concierge will escort, call 1 hour ahead',
        commission_rate: 5.0,
        buyer_agent_commission: 2.5,
        listing_remarks: 'Luxury high-rise living in the heart of downtown. Stunning city and water views from the 28th floor.',
        private_remarks: 'Owner relocating, flexible on closing date',
        virtual_tour_url: 'https://virtualtour.com/condo456',
        video_tour_url: 'https://youtube.com/watch?v=condo456',
        floor_plan_url: 'https://floorplans.com/condo456.pdf'
    },
    '2024-07-03 09:15:00',
    '2024-07-15 16:45:00',
    'mls',
    92
);

UPDATE customers
SET home_address = {"street": "123 Main St", "city": "Seattle", "state": "WA", "zip": "98109"}
WHERE customer_id = 'cust_123';


Select * from properties where property_id = 33333333-3333-3333-3333-333333333333;

update properties
    set address = {
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
        timezone: 'PST',
        neighborhood: 'Downtown',
        subdivision: 'Metropolitan Tower'
    }
where property_id = 33333333-3333-3333-3333-333333333333

UPDATE customers
SET home_address = {"street": "123 Main St", "city": "Seattle", "state": "WA", "zip": "98109"}
WHERE customer_id = 'cust_123'
IF home_address.zip = '98101';

update properties
    set address = {
        street_number: '60',
        street_name: 'Pine Street',
        unit_number: '2801',
         subdivision: 'Metropolitan Tower'
    }
where property_id = 33333333-3333-3333-3333-333333333333
if address.street_number = '500';

Select address.street_name, address.street_number, address.city from properties where property_id = 33333333-3333-3333-3333-333333333333;

update properties
    set address = {
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
        timezone: 'PST',
        neighborhood: 'Downtown',
        subdivision: 'Metropolitan Tower'
    }
where property_id = 33333333-3333-3333-3333-333333333333
if address.street_number = '500';



CREATE TABLE IF NOT EXISTS properties (
    property_id UUID PRIMARY KEY,
    mls_number TEXT,
    address frozen<address_details>,
    listing frozen<listing_details>,
    location frozen<location_quality>,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    data_source TEXT,
    data_quality_score INT
);
