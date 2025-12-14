-- Sample data for Lab4 testing
-- Run this after create_lab4.sql
SET SEARCH_PATH TO Lab4;

-- Insert Passengers
INSERT INTO Passenger (passengerID, passengerName, email, dateOfBirth, frequentFlyer) VALUES
(1, 'Alice Smith', 'alice@email.com', '1990-05-15', 'Y'),
(2, 'Bob Johnson', 'bob@email.com', '1985-08-22', 'N'),
(3, 'Charlie Brown', 'charlie@email.com', '1992-11-30', 'Y'),
(4, 'Diana Prince', 'diana@email.com', '1988-03-10', 'N'),
(5, 'Eve Adams', 'eve@email.com', '1995-07-18', 'N'),
(6, 'Frank Miller', 'frank@email.com', '1980-01-22', 'N'),
(7, 'Grace Lee', 'grace@email.com', '1993-04-12', 'N'),
(8, 'Hank Green', 'hank@email.com', '1985-09-15', 'N'),
(9, 'Ivy White', 'ivy@email.com', '1990-02-20', 'N');


-- Insert Airports
INSERT INTO Airport (airportCode, city, country, numRunways, avgDelayMinutes) VALUES
('LAX', 'Los Angeles', 'USA', 4, 15),
('JFK', 'New York', 'USA', 4, 20),
('SFO', 'San Francisco', 'USA', 3, 12),
('ORD', 'Chicago', 'USA', 7, 18),
('SEA', 'Seattle', 'USA', 3, 10),
('SJC', 'San Jose', 'USA', 2, 10),
('ATL', 'Atlanta', 'USA', 8, 10),
('DEN', 'Denver', 'USA', 4, 10);


-- Insert Flights
INSERT INTO Flight (flightID, departureAirport, arrivalAirport, scheduledDeparture, scheduledArrival, actualDeparture, actualArrival, aircraftType) VALUES
(1, 'LAX', 'JFK', '2025-12-15 08:00:00', '2025-12-15 16:30:00', '2025-12-15 08:15:00', '2025-12-15 16:45:00', 'Boeing 777'),
(2, 'SFO', 'ORD', '2025-12-20 10:00:00', '2025-12-20 16:00:00', '2025-12-20 10:05:00', '2025-12-20 16:10:00', 'Airbus A320'),
(3, 'JFK', 'LAX', '2026-01-10 09:00:00', '2026-01-10 12:30:00', '2026-01-10 09:20:00', '2026-01-10 12:50:00', 'Boeing 737'),
(4, 'SEA', 'SFO', '2025-12-25 14:00:00', '2025-12-25 16:30:00', '2025-12-25 14:10:00', '2025-12-25 16:40:00', 'Airbus A380'),
(5, 'ORD', 'SEA', '2025-10-08 11:00:00', '2025-10-09 13:30:00', '2026-02-01 11:15:00', '2026-02-01 13:45:00',  'Airbus A380'),
(6, 'SJC', 'DEN', '2025-10-09 11:00:00', '2025-10-09 13:30:00', '2026-02-01 11:15:00', '2026-02-01 13:45:00',  'Airbus A380'),
(7, 'ATL', 'DEN', '2025-10-10 11:00:00', '2025-10-11 13:30:00', '2026-02-01 11:15:00', '2026-02-01 13:45:00',  'Airbus A380');

-- Insert CrewMembers
INSERT INTO CrewMember (crewID, crewName, crewRole, yearsExperience) VALUES
(1, 'Captain Miller', 'Pilot', 15),
(2, 'First Officer Jones', 'Co-Pilot', 8),
(3, 'Sarah Wilson', 'Flight Attendant', 5),
(4, 'Mike Davis', 'Flight Attendant', 3),
(5, 'Captain Anderson', 'Pilot', 20);

-- Insert Reservations
INSERT INTO Reservation (reservationID, passengerID, flightID, bookingDate, seatClass, ticketPrice, paymentMethod) VALUES
(1, 1, 1, '2025-10-15', 'E', 450.00, 'V'),
(2, 4, 6, '2025-11-15', 'E', 300.00, 'V'),
(3, 6, 5, '2025-11-18', 'E', 900.00, 'M'),
(4, 2, 5, '2025-10-01', 'B', 1200.00, 'M'),
(5, 3, 4, '2025-10-25', 'E', 280.00, 'V'),
(6, 3, 3, '2025-11-20', 'F', 2500.00, 'V'),
(7, 4, 5, '2025-12-01', 'B', 680.00, 'D'),
(8, 5, 2, '2025-11-25', 'E', 320.00, 'M'),
(9, 2, 6, '2025-12-05', 'E', 400.00, 'A'),
(10, 5, 7, '2025-12-10', 'F', 1500.00, 'V'),
(11, 1, 5, '2025-12-12', 'E', 700.00, 'A'),
(12, 2, 7, '2025-12-14', 'E', 1300.00, 'D'),
(13, 6, 5, '2025-12-16', 'B', 350.00, 'M'),
(14, 7, 4, '2025-12-18', 'F', 2000.00, 'V'),
(15, 8, 4, '2025-12-19', 'E', 450.00, 'A'),
(16, 9, 7, '2025-12-20', 'F', 800.00, 'D'),
(17, 2, 1, '2025-10-15', 'E', 450.00, 'V'),
(18, 7, 5, '2025-10-01', 'E', 1200.00, 'M'),
(19, 8, 5, '2025-10-01', 'E', 1200.00, 'M'),
(20, 3, 1, '2025-10-15', 'E', 450.00, 'V'),
(21, 4, 1, '2025-10-15', 'B', 450.00, 'V'),
(22, 7, 2, '2025-11-21', 'E', 200.00, 'M'),
(23, 9, 2, '2025-11-22', 'E', 320.00, 'V'),
(24, 1, 3, '2025-11-20', 'F', 2500.00, 'V'),
(25, 5, 3, '2025-11-20', 'F', 2500.00, 'V'),
(26, 6, 4, '2025-12-18', 'F', 2000.00, 'V'),
(27, 8, 6, '2025-12-05', 'E', 400.00, 'A'),
(28, 9, 6, '2025-12-07', 'E', 200.00, 'V'),
(29, 9, 2, '2025-11-25', 'E', 325.00, 'M'),
(30, 7, 6, '2025-01-05', 'E', 430.00, 'A');

-- Insert CheckIns
INSERT INTO CheckIn (reservationID, passengerID, checkInTime, bagCount, seatNumber) VALUES
(1, 1, '2025-12-15 06:30:00', 2, '12A'),
(1, 2, '2025-12-15 06:45:00', 1, '12B'),
(4, 2, '2025-12-20 08:30:00', 1, '24C'),
(6, 3, '2025-12-25 12:30:00', 1, '8B'),
(5, 3, '2025-12-20 08:45:00', 2, '15D'),
(7, 4, '2025-12-25 12:45:00', 0, '20E'),
(8, 5, '2026-01-10 07:30:00', 1, '14C'),
(9, 2, '2026-01-10 07:45:00', 2, '1A'),
(10, 4, '2025-12-15 06:50:00', 0, '22B'),
(17, 2, '2025-12-15 06:55:00', 1, '13A'),
(18, 7, '2025-11-25 08:15:00', 2, '16D');

-- Insert FlightCrewAssignments
INSERT INTO FlightCrewAssignment (flightID, crewID, compensation) VALUES
(1, 1, 500.00),
(1, 2, 350.00),
(1, 3, 200.00),
(2, 5, 450.00),
(2, 4, 180.00),
(3, 1, 500.00),
(3, 3, 200.00),
(4, 1, 500.00),
(4, 2, 350.00),
(4, 3, 200.00),
(5, 5, 450.00),
(5, 4, 180.00),
(6, 1, 500.00),
(6, 3, 200.00),
(6, 4, 200.00);

