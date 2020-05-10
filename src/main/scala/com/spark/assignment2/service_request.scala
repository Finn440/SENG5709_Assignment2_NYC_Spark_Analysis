package com.spark.assignment2

case class service_request(
    unique_key: Int,
    created_date: String,
    closed_date: String,
    agency: String,
    agency_name: String,
    complaint_type: String,
    descriptor: String,
    location_type: String,
    incident_zip: String,
    incident_address: String,
    street_name: String,
    cross_street_1: String,
    cross_street_2: String,
    intersection_street_1: String,
    intersection_street_2: String,
    address_type: String,
    city: String,
    landmark: String,
    facility_type: String,
    status: String,
    due_date: String,
    resolution_description: String,
    resolution_action_updated_date: String,
    community_board: String,
    bbl: Long,
    borough: String,
    x_coordinate_state_plane: String,
    y_coordinate_state_plane: String,
    open_data_channel_type: String,
    park_facility_name: String,
    park_borough: String,
    vehicle_type: String,
    taxi_company_borough: String,
    taxi_pick_up_location: String,
    bridge_highway_name: String,
    bridge_highway_direction: String,
    road_ramp: String,
    bridge_highway_segment: String,
    latitude: Double,
    longitude: Double,
    location: String
)
