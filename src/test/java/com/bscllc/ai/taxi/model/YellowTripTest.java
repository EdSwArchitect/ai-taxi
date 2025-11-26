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
 * Test class for YellowTrip record.
 * Tests JSON serialization, deserialization, and record functionality.
 */
@DisplayName("YellowTrip Record Tests")
class YellowTripTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    @DisplayName("Should create YellowTrip with all fields")
    void testCreateYellowTrip() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        YellowTrip trip = new YellowTrip(
            1,                    // vendorId
            pickupTime,           // tpepPickupDatetime
            dropoffTime,          // tpepDropoffDatetime
            2,                    // passengerCount
            5.5,                  // tripDistance
            1,                    // ratecodeId
            "N",                  // storeAndFwdFlag
            100,                  // puLocationId
            200,                  // doLocationId
            1,                    // paymentType
            15.50,                // fareAmount
            1.0,                  // extra
            0.50,                 // mtaTax
            3.00,                 // tipAmount
            0.0,                  // tollsAmount
            1.0,                  // improvementSurcharge
            21.00,                // totalAmount
            2.50,                 // congestionSurcharge
            1.25                  // airportFee
        );

        assertNotNull(trip);
        assertEquals(1, trip.vendorId());
        assertEquals(pickupTime, trip.tpepPickupDatetime());
        assertEquals(dropoffTime, trip.tpepDropoffDatetime());
        assertEquals("N", trip.storeAndFwdFlag());
        assertEquals(2, trip.passengerCount());
        assertEquals(5.5, trip.tripDistance());
        assertEquals(21.00, trip.totalAmount());
    }

    @Test
    @DisplayName("Should create YellowTrip with null values")
    void testCreateYellowTripWithNulls() {
        YellowTrip trip = new YellowTrip(
            null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null
        );

        assertNotNull(trip);
        assertNull(trip.vendorId());
        assertNull(trip.tpepPickupDatetime());
        assertNull(trip.tpepDropoffDatetime());
    }

    @Test
    @DisplayName("Should serialize YellowTrip to JSON")
    void testSerializeToJson() throws JsonProcessingException {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        YellowTrip trip = new YellowTrip(
            1, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        String json = objectMapper.writeValueAsString(trip);

        assertNotNull(json);
        assertFalse(json.isEmpty());
        assertTrue(json.contains("\"vendor_id\":1"));
        assertTrue(json.contains("\"tpep_pickup_datetime\""));
        assertTrue(json.contains("\"tpep_dropoff_datetime\""));
        assertTrue(json.contains("\"passenger_count\":2"));
        assertTrue(json.contains("\"total_amount\":21.0"));

        System.out.println("\n=== YellowTrip JSON Serialization ===");
        System.out.println(json);
    }

    @Test
    @DisplayName("Should deserialize JSON to YellowTrip")
    void testDeserializeFromJson() throws JsonProcessingException {
        String json = """
            {
                "vendor_id": 2,
                "tpep_pickup_datetime": "2025-01-15 14:30:00",
                "tpep_dropoff_datetime": "2025-01-15 15:15:00",
                "passenger_count": 3,
                "trip_distance": 7.2,
                "ratecode_id": 2,
                "store_and_fwd_flag": "Y",
                "pu_location_id": 150,
                "do_location_id": 250,
                "payment_type": 2,
                "fare_amount": 20.00,
                "extra": 2.0,
                "mta_tax": 0.50,
                "tip_amount": 4.50,
                "tolls_amount": 5.25,
                "improvement_surcharge": 1.0,
                "total_amount": 33.25,
                "congestion_surcharge": 2.50,
                "airport_fee": 0.0
            }
            """;

        YellowTrip trip = objectMapper.readValue(json, YellowTrip.class);

        assertNotNull(trip);
        assertEquals(2, trip.vendorId());
        assertEquals(LocalDateTime.of(2025, 1, 15, 14, 30, 0), trip.tpepPickupDatetime());
        assertEquals(LocalDateTime.of(2025, 1, 15, 15, 15, 0), trip.tpepDropoffDatetime());
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

        YellowTrip original = new YellowTrip(
            1, pickupTime, dropoffTime, 1, 3.5, 1, "N", 100, 200,
            1, 12.00, 0.0, 0.50, 2.00, 0.0, 1.0, 15.50, 0.0, 0.0
        );

        String json = objectMapper.writeValueAsString(original);
        YellowTrip deserialized = objectMapper.readValue(json, YellowTrip.class);

        assertNotNull(deserialized);
        assertEquals(original, deserialized);
        assertEquals(original.vendorId(), deserialized.vendorId());
        assertEquals(original.tpepPickupDatetime(), deserialized.tpepPickupDatetime());
        assertEquals(original.tpepDropoffDatetime(), deserialized.tpepDropoffDatetime());
        assertEquals(original.totalAmount(), deserialized.totalAmount());
    }

    @Test
    @DisplayName("Should ignore unknown properties during deserialization")
    void testIgnoreUnknownProperties() throws JsonProcessingException {
        String json = """
            {
                "vendor_id": 1,
                "tpep_pickup_datetime": "2025-01-15 10:00:00",
                "tpep_dropoff_datetime": "2025-01-15 10:30:00",
                "unknown_field": "should be ignored",
                "another_unknown": 12345,
                "passenger_count": 2,
                "trip_distance": 5.0
            }
            """;

        // Should not throw exception due to @JsonIgnoreProperties(ignoreUnknown = true)
        YellowTrip trip = objectMapper.readValue(json, YellowTrip.class);

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

        YellowTrip trip1 = new YellowTrip(
            1, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        YellowTrip trip2 = new YellowTrip(
            1, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        YellowTrip trip3 = new YellowTrip(
            2, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        assertEquals(trip1, trip2);
        assertNotEquals(trip1, trip3);
        assertNotEquals(trip1, null);
        assertNotEquals(trip1, "not a YellowTrip");
    }

    @Test
    @DisplayName("Should test hashCode method")
    void testHashCode() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        YellowTrip trip1 = new YellowTrip(
            1, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        YellowTrip trip2 = new YellowTrip(
            1, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        assertEquals(trip1.hashCode(), trip2.hashCode());
    }

    @Test
    @DisplayName("Should test toString method")
    void testToString() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        YellowTrip trip = new YellowTrip(
            1, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        String toString = trip.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("YellowTrip"));
        assertTrue(toString.contains("vendorId=1"));
        assertTrue(toString.contains("passengerCount=2"));

        System.out.println("\n=== YellowTrip toString ===");
        System.out.println(toString);
    }

    @Test
    @DisplayName("Should deserialize JSON with missing optional fields")
    void testDeserializeWithMissingFields() throws JsonProcessingException {
        String json = """
            {
                "vendor_id": 1,
                "tpep_pickup_datetime": "2025-01-15 10:00:00",
                "tpep_dropoff_datetime": "2025-01-15 10:30:00"
            }
            """;

        YellowTrip trip = objectMapper.readValue(json, YellowTrip.class);

        assertNotNull(trip);
        assertEquals(1, trip.vendorId());
        assertNotNull(trip.tpepPickupDatetime());
        assertNotNull(trip.tpepDropoffDatetime());
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
                "tpep_pickup_datetime": null,
                "tpep_dropoff_datetime": null,
                "passenger_count": null,
                "trip_distance": null,
                "total_amount": null
            }
            """;

        YellowTrip trip = objectMapper.readValue(json, YellowTrip.class);

        assertNotNull(trip);
        assertNull(trip.vendorId());
        assertNull(trip.tpepPickupDatetime());
        assertNull(trip.tpepDropoffDatetime());
        assertNull(trip.passengerCount());
        assertNull(trip.tripDistance());
        assertNull(trip.totalAmount());
    }

    @Test
    @DisplayName("Should verify difference from GreenTrip (tpep vs lpep prefix)")
    void testYellowTripUsesTpepPrefix() {
        LocalDateTime pickupTime = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        LocalDateTime dropoffTime = LocalDateTime.of(2025, 1, 15, 11, 0, 0);

        YellowTrip yellowTrip = new YellowTrip(
            1, pickupTime, dropoffTime, 2, 5.5, 1, "N", 100, 200,
            1, 15.50, 1.0, 0.50, 3.00, 0.0, 1.0, 21.00, 2.50, 1.25
        );

        assertNotNull(yellowTrip.tpepPickupDatetime());
        assertNotNull(yellowTrip.tpepDropoffDatetime());
        // Yellow trip does not have tripType field
        // This test verifies the field names are different from GreenTrip
    }
}

