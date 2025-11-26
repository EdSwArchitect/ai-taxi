package com.bscllc.ai.taxi.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for GreenTrip record.
 * Tests JSON serialization, deserialization, and record functionality.
 */
@DisplayName("GreenTrip Record Tests")
class GreenTripTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    @DisplayName("Should create GreenTrip with all fields")
    void testCreateGreenTrip() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        GreenTrip trip = new GreenTrip(
            1,                    // vendorId
            pickupTime,           // lpepPickupDatetime
            dropoffTime,          // lpepDropoffDatetime
            "N",                  // storeAndFwdFlag
            1,                    // ratecodeId
            100,                  // puLocationId
            200,                  // doLocationId
            2,                    // passengerCount
            5.5,                  // tripDistance
            15.50,                // fareAmount
            1.0,                  // extra
            0.50,                 // mtaTax
            3.00,                 // tipAmount
            0.0,                  // tollsAmount
            1.0,                  // improvementSurcharge
            21.00,                // totalAmount
            1,                    // paymentType
            1,                    // tripType
            2.50,                 // congestionSurcharge
            1.25                  // airportFee
        );

        assertNotNull(trip);
        assertEquals(1, trip.vendorId());
        assertEquals(pickupTime, trip.lpepPickupDatetime());
        assertEquals(dropoffTime, trip.lpepDropoffDatetime());
        assertEquals("N", trip.storeAndFwdFlag());
        assertEquals(2, trip.passengerCount());
        assertEquals(5.5, trip.tripDistance());
        assertEquals(21.00, trip.totalAmount());
    }

    @Test
    @DisplayName("Should create GreenTrip with null values")
    void testCreateGreenTripWithNulls() {
        GreenTrip trip = new GreenTrip(
            null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null,
            null, null
        );

        assertNotNull(trip);
        assertNull(trip.vendorId());
        assertNull(trip.lpepPickupDatetime());
        assertNull(trip.lpepDropoffDatetime());
    }

    @Test
    @DisplayName("Should serialize GreenTrip to JSON")
    void testSerializeToJson() throws JsonProcessingException {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        GreenTrip trip = new GreenTrip(
            1, pickupTime, dropoffTime, "N", 1, 100, 200,
            2, 5.5, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0,
            21.00, 1, 1, 2.50, 1.25
        );

        String json = objectMapper.writeValueAsString(trip);

        assertNotNull(json);
        assertFalse(json.isEmpty());
        assertTrue(json.contains("\"vendor_id\":1"));
        assertTrue(json.contains("\"lpep_pickup_datetime\""));
        assertTrue(json.contains("\"lpep_dropoff_datetime\""));
        assertTrue(json.contains("\"passenger_count\":2"));
        assertTrue(json.contains("\"total_amount\":21.0"));

        System.out.println("\n=== GreenTrip JSON Serialization ===");
        System.out.println(json);
    }

    @Test
    @DisplayName("Should deserialize JSON to GreenTrip")
    void testDeserializeFromJson() throws JsonProcessingException {
        String json = """
            {
                "vendor_id": 2,
                "lpep_pickup_datetime": "2025-01-15 14:30:00",
                "lpep_dropoff_datetime": "2025-01-15 15:15:00",
                "store_and_fwd_flag": "Y",
                "ratecode_id": 2,
                "pu_location_id": 150,
                "do_location_id": 250,
                "passenger_count": 3,
                "trip_distance": 7.2,
                "fare_amount": 20.00,
                "extra": 2.0,
                "mta_tax": 0.50,
                "tip_amount": 4.50,
                "tolls_amount": 5.25,
                "improvement_surcharge": 1.0,
                "total_amount": 33.25,
                "payment_type": 2,
                "trip_type": 1,
                "congestion_surcharge": 2.50,
                "airport_fee": 0.0
            }
            """;

        GreenTrip trip = objectMapper.readValue(json, GreenTrip.class);

        assertNotNull(trip);
        assertEquals(2, trip.vendorId());
        assertEquals(LocalDateTime.of(2025, 1, 15, 14, 30, 0), trip.lpepPickupDatetime());
        assertEquals(LocalDateTime.of(2025, 1, 15, 15, 15, 0), trip.lpepDropoffDatetime());
        assertEquals("Y", trip.storeAndFwdFlag());
        assertEquals(3, trip.passengerCount());
        assertEquals(7.2, trip.tripDistance());
        assertEquals(33.25, trip.totalAmount());
    }

    @Test
    @DisplayName("Should handle round-trip JSON serialization/deserialization")
    void testRoundTripJson() throws JsonProcessingException {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 2, 20, 9, 15, 30);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 2, 20, 9, 45, 45);

        GreenTrip original = new GreenTrip(
            1, pickupTime, dropoffTime, "N", 1, 100, 200,
            1, 3.5, 12.00, 0.0, 0.50, 2.00, 0.0, 1.0,
            15.50, 1, 1, 0.0, 0.0
        );

        String json = objectMapper.writeValueAsString(original);
        GreenTrip deserialized = objectMapper.readValue(json, GreenTrip.class);

        assertNotNull(deserialized);
        assertEquals(original, deserialized);
        assertEquals(original.vendorId(), deserialized.vendorId());
        assertEquals(original.lpepPickupDatetime(), deserialized.lpepPickupDatetime());
        assertEquals(original.lpepDropoffDatetime(), deserialized.lpepDropoffDatetime());
        assertEquals(original.totalAmount(), deserialized.totalAmount());
    }

    @Test
    @DisplayName("Should ignore unknown properties during deserialization")
    void testIgnoreUnknownProperties() throws JsonProcessingException {
        String json = """
            {
                "vendor_id": 1,
                "lpep_pickup_datetime": "2025-01-15 10:00:00",
                "lpep_dropoff_datetime": "2025-01-15 10:30:00",
                "unknown_field": "should be ignored",
                "another_unknown": 12345,
                "passenger_count": 2,
                "trip_distance": 5.0
            }
            """;

        // Should not throw exception due to @JsonIgnoreProperties(ignoreUnknown = true)
        GreenTrip trip = objectMapper.readValue(json, GreenTrip.class);

        assertNotNull(trip);
        assertEquals(1, trip.vendorId());
        assertEquals(2, trip.passengerCount());
        assertEquals(5.0, trip.tripDistance());
    }

    @Test
    @DisplayName("Should test equals method")
    void testEquals() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        GreenTrip trip1 = new GreenTrip(
            1, pickupTime, dropoffTime, "N", 1, 100, 200,
            2, 5.5, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0,
            21.00, 1, 1, 2.50, 1.25
        );

        GreenTrip trip2 = new GreenTrip(
            1, pickupTime, dropoffTime, "N", 1, 100, 200,
            2, 5.5, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0,
            21.00, 1, 1, 2.50, 1.25
        );

        GreenTrip trip3 = new GreenTrip(
            2, pickupTime, dropoffTime, "N", 1, 100, 200,
            2, 5.5, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0,
            21.00, 1, 1, 2.50, 1.25
        );

        assertEquals(trip1, trip2);
        assertNotEquals(trip1, trip3);
        assertNotEquals(trip1, null);
        assertNotEquals(trip1, "not a GreenTrip");
    }

    @Test
    @DisplayName("Should test hashCode method")
    void testHashCode() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        GreenTrip trip1 = new GreenTrip(
            1, pickupTime, dropoffTime, "N", 1, 100, 200,
            2, 5.5, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0,
            21.00, 1, 1, 2.50, 1.25
        );

        GreenTrip trip2 = new GreenTrip(
            1, pickupTime, dropoffTime, "N", 1, 100, 200,
            2, 5.5, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0,
            21.00, 1, 1, 2.50, 1.25
        );

        assertEquals(trip1.hashCode(), trip2.hashCode());
    }

    @Test
    @DisplayName("Should test toString method")
    void testToString() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        GreenTrip trip = new GreenTrip(
            1, pickupTime, dropoffTime, "N", 1, 100, 200,
            2, 5.5, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0,
            21.00, 1, 1, 2.50, 1.25
        );

        String toString = trip.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("GreenTrip"));
        assertTrue(toString.contains("vendorId=1"));
        assertTrue(toString.contains("passengerCount=2"));

        System.out.println("\n=== GreenTrip toString ===");
        System.out.println(toString);
    }

    @Test
    @DisplayName("Should deserialize JSON with missing optional fields")
    void testDeserializeWithMissingFields() throws JsonProcessingException {
        String json = """
            {
                "vendor_id": 1,
                "lpep_pickup_datetime": "2025-01-15 10:00:00",
                "lpep_dropoff_datetime": "2025-01-15 10:30:00"
            }
            """;

        GreenTrip trip = objectMapper.readValue(json, GreenTrip.class);

        assertNotNull(trip);
        assertEquals(1, trip.vendorId());
        assertNotNull(trip.lpepPickupDatetime());
        assertNotNull(trip.lpepDropoffDatetime());
        // Other fields should be null
        assertNull(trip.passengerCount());
        assertNull(trip.tripDistance());
    }

    @Test
    @DisplayName("Should handle JSON with null values")
    void testDeserializeWithNullValues() throws JsonProcessingException {
        String json = """
            {
                "vendor_id": null,
                "lpep_pickup_datetime": null,
                "lpep_dropoff_datetime": null,
                "passenger_count": null,
                "trip_distance": null,
                "total_amount": null
            }
            """;

        GreenTrip trip = objectMapper.readValue(json, GreenTrip.class);

        assertNotNull(trip);
        assertNull(trip.vendorId());
        assertNull(trip.lpepPickupDatetime());
        assertNull(trip.lpepDropoffDatetime());
        assertNull(trip.passengerCount());
        assertNull(trip.tripDistance());
        assertNull(trip.totalAmount());
    }
}

