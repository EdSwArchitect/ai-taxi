package com.bscllc.ai.taxi.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.LocalDateTime;

/**
 * Record class representing a Green Taxi trip from NYC green_tripdata Parquet files.
 * 
 * This record is annotated for JSON serialization and deserialization using Jackson.
 * Fields correspond to the schema in green_tripdata_2025_01.parquet.
 * 
 * The record ignores unknown properties during JSON deserialization to allow for
 * schema evolution and variations in the Parquet file structure.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "vendorId", "lpepPickupDatetime", "lpepDropoffDatetime", "storeAndFwdFlag",
    "ratecodeId", "puLocationId", "doLocationId", "passengerCount",
    "tripDistance", "fareAmount", "extra", "mtaTax", "tipAmount",
    "tollsAmount", "improvementSurcharge", "totalAmount", "paymentType",
    "tripType", "congestionSurcharge", "airportFee"
})
public record GreenTrip(
    @JsonProperty("vendor_id")
    Integer vendorId,
    
    @JsonProperty("lpep_pickup_datetime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", shape = JsonFormat.Shape.STRING)
    LocalDateTime lpepPickupDatetime,
    
    @JsonProperty("lpep_dropoff_datetime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", shape = JsonFormat.Shape.STRING)
    LocalDateTime lpepDropoffDatetime,
    
    @JsonProperty("store_and_fwd_flag")
    String storeAndFwdFlag,
    
    @JsonProperty("ratecode_id")
    Integer ratecodeId,
    
    @JsonProperty("pu_location_id")
    Integer puLocationId,
    
    @JsonProperty("do_location_id")
    Integer doLocationId,
    
    @JsonProperty("passenger_count")
    Integer passengerCount,
    
    @JsonProperty("trip_distance")
    Double tripDistance,
    
    @JsonProperty("fare_amount")
    Double fareAmount,
    
    @JsonProperty("extra")
    Double extra,
    
    @JsonProperty("mta_tax")
    Double mtaTax,
    
    @JsonProperty("tip_amount")
    Double tipAmount,
    
    @JsonProperty("tolls_amount")
    Double tollsAmount,
    
    @JsonProperty("improvement_surcharge")
    Double improvementSurcharge,
    
    @JsonProperty("total_amount")
    Double totalAmount,
    
    @JsonProperty("payment_type")
    Integer paymentType,
    
    @JsonProperty("trip_type")
    Integer tripType,
    
    @JsonProperty("congestion_surcharge")
    Double congestionSurcharge,
    
    @JsonProperty("airport_fee")
    Double airportFee
) {
    /**
     * Creates a GreenTrip record with all fields.
     * 
     * @param vendorId Vendor ID
     * @param lpepPickupDatetime Pickup date/time
     * @param lpepDropoffDatetime Dropoff date/time
     * @param storeAndFwdFlag Store and forward flag
     * @param ratecodeId Rate code ID
     * @param puLocationId Pickup location ID
     * @param doLocationId Dropoff location ID
     * @param passengerCount Passenger count
     * @param tripDistance Trip distance in miles
     * @param fareAmount Fare amount
     * @param extra Extra charges
     * @param mtaTax MTA tax
     * @param tipAmount Tip amount
     * @param tollsAmount Tolls amount
     * @param improvementSurcharge Improvement surcharge
     * @param totalAmount Total amount
     * @param paymentType Payment type
     * @param tripType Trip type
     * @param congestionSurcharge Congestion surcharge
     * @param airportFee Airport fee
     */
    public GreenTrip {
        // Compact constructor for validation if needed
    }
}

