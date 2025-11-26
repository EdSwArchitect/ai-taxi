package com.bscllc.ai.taxi.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.LocalDateTime;

/**
 * Record class representing a Yellow Taxi trip from NYC yellow_tripdata Parquet files.
 * 
 * This record is annotated for JSON serialization and deserialization using Jackson.
 * Fields correspond to the schema in yellow_tripdata_2025_01.parquet.
 * 
 * The record ignores unknown properties during JSON deserialization to allow for
 * schema evolution and variations in the Parquet file structure.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "vendorId", "tpepPickupDatetime", "tpepDropoffDatetime", "passengerCount",
    "tripDistance", "ratecodeId", "storeAndFwdFlag", "puLocationId",
    "doLocationId", "paymentType", "fareAmount", "extra", "mtaTax",
    "tipAmount", "tollsAmount", "improvementSurcharge", "totalAmount",
    "congestionSurcharge", "airportFee"
})
public record YellowTrip(
    @JsonProperty("vendor_id")
    Integer vendorId,
    
    @JsonProperty("tpep_pickup_datetime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", shape = JsonFormat.Shape.STRING)
    LocalDateTime tpepPickupDatetime,
    
    @JsonProperty("tpep_dropoff_datetime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", shape = JsonFormat.Shape.STRING)
    LocalDateTime tpepDropoffDatetime,
    
    @JsonProperty("passenger_count")
    Integer passengerCount,
    
    @JsonProperty("trip_distance")
    Double tripDistance,
    
    @JsonProperty("ratecode_id")
    Integer ratecodeId,
    
    @JsonProperty("store_and_fwd_flag")
    String storeAndFwdFlag,
    
    @JsonProperty("pu_location_id")
    Integer puLocationId,
    
    @JsonProperty("do_location_id")
    Integer doLocationId,
    
    @JsonProperty("payment_type")
    Integer paymentType,
    
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
    
    @JsonProperty("congestion_surcharge")
    Double congestionSurcharge,
    
    @JsonProperty("airport_fee")
    Double airportFee
) {
    /**
     * Creates a YellowTrip record with all fields.
     * 
     * @param vendorId Vendor ID
     * @param tpepPickupDatetime Pickup date/time (TPEP = Taxi & Limousine Commission Passenger Enhancement Program)
     * @param tpepDropoffDatetime Dropoff date/time
     * @param passengerCount Passenger count
     * @param tripDistance Trip distance in miles
     * @param ratecodeId Rate code ID
     * @param storeAndFwdFlag Store and forward flag
     * @param puLocationId Pickup location ID
     * @param doLocationId Dropoff location ID
     * @param paymentType Payment type
     * @param fareAmount Fare amount
     * @param extra Extra charges
     * @param mtaTax MTA tax
     * @param tipAmount Tip amount
     * @param tollsAmount Tolls amount
     * @param improvementSurcharge Improvement surcharge
     * @param totalAmount Total amount
     * @param congestionSurcharge Congestion surcharge
     * @param airportFee Airport fee
     */
    public YellowTrip {
        // Compact constructor for validation if needed
    }
}

