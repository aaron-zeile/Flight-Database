-- -- Optional (not required to include in submission):
DROP SCHEMA Lab4 CASCADE;
CREATE SCHEMA Lab4;

-- Passenger(passengerID, passengerName, email, dateOfBirth, frequentFlyer)
CREATE TABLE Passenger(
  passengerID INT PRIMARY KEY,
  passengerName VARCHAR(40) NOT NULL,
  email VARCHAR(40),
  dateOfBirth DATE,
  frequentFlyer CHAR(1),
  UNIQUE (passengerName, email)
);

-- Airport(airportCode, city, country, numRunways, avgDelayMinutes)
CREATE TABLE Airport(
  airportCode VARCHAR(40) PRIMARY KEY,
  city VARCHAR(40) NOT NULL,
  country VARCHAR(40),
  numRunways INT,
  avgDelayMinutes INT,
  UNIQUE (city, country)
);

-- Flight(flightID, departureAirport, arrivalAirport,
--        scheduledDeparture, scheduledArrival,
--        actualDeparture, actualArrival, aircraftType)
CREATE TABLE Flight(
  flightID INT PRIMARY KEY,
  departureAirport VARCHAR(40) REFERENCES Airport(airportCode),
  arrivalAirport   VARCHAR(40) REFERENCES Airport(airportCode),
  scheduledDeparture TIMESTAMP,
  scheduledArrival   TIMESTAMP,
  actualDeparture    TIMESTAMP,
  actualArrival      TIMESTAMP,
  aircraftType VARCHAR(40) NOT NULL
);

-- CrewMember(crewID, crewName, role, yearsExperience)
CREATE TABLE CrewMember(
  crewID INT PRIMARY KEY,
  crewName VARCHAR(40) NOT NULL,
  crewRole VARCHAR(40),
  yearsExperience INT,
  UNIQUE (crewName)
);

-- Reservation(reservationID, passengerID, flightID, bookingDate, seatClass, ticketPrice, paymentMethod)
CREATE TABLE Reservation(
  reservationID INT PRIMARY KEY,
  passengerID INT REFERENCES Passenger(passengerID),
  flightID    INT REFERENCES Flight(flightID),
  bookingDate DATE,
  seatClass CHAR(1),
  ticketPrice NUMERIC(9,2),
  paymentMethod VARCHAR(40),
  UNIQUE (bookingDate, paymentMethod, passengerID)
);

-- CheckIn(reservationID, passengerID, checkInTime, bagCount, seatNumber)
CREATE TABLE CheckIn(
  reservationID INT REFERENCES Reservation(reservationID),
  passengerID   INT REFERENCES Passenger(passengerID),
  checkInTime TIMESTAMP,
  bagCount INT,
  seatNumber VARCHAR(40),
  PRIMARY KEY (reservationID, passengerID)
);

-- FlightCrewAssignment(flightID, crewID, compensation)
CREATE TABLE FlightCrewAssignment(
  flightID INT REFERENCES Flight(flightID),
  crewID   INT REFERENCES CrewMember(crewID),
  compensation NUMERIC(9,2),
  PRIMARY KEY (flightID, crewID)
);
