#! /usr/bin/env python

#  runAirlineApplication Solution
from datetime import datetime
import psycopg2, sys

# The three Python functions for Lab4 should appear below.
# Write those functions, as described in Lab4 Section 5 (and Section 6,
# which describes the Stored Function used by the third Python function).
#
# Write the tests of those function in main, as described in Section 7
# of Lab4.


 # countNumberOfDepartingPassengers (myConn, departureAirport):
 # The airline needs to know how many unique customers have departed from a given airport.
 # The Reservation table tells us about reservations made by a passenger(passengerID) for a given flight(flightID), while the Flight table tells us about the flights(flightID) that departed from a given airport(departureAirport).
 #
 # Besides the database connection, the countNumberOfDepartingPassengers function has one parameter, an integer, departureAirport.
 #
 # We want to count the number of different passengers that left a particular airport. The function countNumberOfDepartingPassengers() should count the number of different passengers who have departed an airport by the parameter departureAirport. The function should return the count.
 #
 # countNumberOfDepartingPassengers returns that value.
 #
 # For more details, including error handling and return codes, see the Lab4 pdf.

def countNumberOfDepartingPassengers (myConn, departureAirport):
    cur = myConn.cursor()
    data = (departureAirport,)

    # check if departureAirport exists in airportCodes
    cur.execute('''SELECT 1
               FROM Airport
               WHERE airportCode = (%s);''', data)
    # if it doesn't exist
    if (cur.fetchone() is None):
        cur.close()
        return -1

    # check if departureAirport has any departing passengers
    cur.execute('''SELECT 1
                FROM Flight f, Reservation r
                WHERE f.departureAirport = (%s) AND
                      f.flightID = r.flightID''', data)
    # if it does not
    if (cur.fetchone() is None):
        cur.close()
        return 0

    # if airportCode exists, get unique customers
    cur.execute('''SELECT DISTINCT r.passengerID
                FROM Reservation r, Flight f
                WHERE r.flightID = f.flightID AND
                f.departureAirport = (%s);''', data)
    rows = cur.fetchall()
    cur.close()
    return len(rows)
# end countNumberOfDepartingPassengers


# updateReservationPayment (myConn, departureDate):
# In the Reservation table, the value of paymentMethod corresponds to the method that a passenger used to pay for the reservation. These correspond to credit codes (V, M, A, D). These are the only values in the Lab4 load data.
# Besides the database connection, updateReservationPayment has another parameter, departureDate, which is a string formatted as YEAR-MONTH-DAY or YYYY-MM-DD.
# If departureDate matches the date of the scheduledDeparture timestamp field in the Flight table, then the paymentMethod field in the Reservation table should have ‘REIMBURSED <departureDate>’ appended at the end. 
#If the year in departureDate is less than(<) 2025 or greater than (>) 2025, then no changes should be made to any status values, and updateReservationPayment should return -1.
#
# updateReservationPayment should return the number of reservations whose payment method was updated.
#
# For more details, including error handling, see the Lab4 pdf.

def updateReservationPayment (myConn, departureDate):
    # check if year is valid
    try:
        year = int(departureDate.split("-")[0])
    except:
        return -1
    
    if year < 2025 or year > 2025:
        return -1
    
    # perform update
    cur = myConn.cursor()
    sql =       '''
                UPDATE Reservation r
                SET paymentMethod = r.paymentMethod || ' REIMBURSED ' || %s
                FROM Flight f
                WHERE r.flightID = f.flightID AND
                      DATE(f.scheduledDeparture) = (%s)
                '''
    
    cur.execute(sql, (departureDate, departureDate))
    num_updated = cur.rowcount

    cur.close()
    return num_updated

# end updateOrderStatus


# promoteCrewMembers(crewAssignments, minYearsExperience)

# The airlines want’s to promote crew members according to new seniority policy reliant on number of assignments and years of experience
#
# Besides the database connection, this Python function has two other parameters, crewAssignments and minYearsExperience which are both integers.
#
# promoteCrewMembers invokes a Stored Function, promoteCrewMembersFunction, that you will need to implement and store in the database according to the description in Section 6. The stored function promoteCrewMembersFunction has the same parameters assignments and years as the promoteCrewMembers (but the database connection is not a parameters for the stored function), and promoteCrewMembersFunction returns an integer.
#
# Section 6 tells you how to apply the promotions, and explains the integer value that promoteCrewMembersFunction returns. The promoteCrewMembers Python function returns the same integer value that the promoteCrewMembersFunction stored function returns.
#
# promoteCrewMembersFunction doesn’t print anything. The promoteCrewMembers function must only invoke the stored function promoteCrewMembersFunction, which does all of the work for this part of the assignment; promoteCrewMembers should not do any of the work itself.

#
# For more details, see the Lab4 pdf.

def promoteCrewMembers (myConn, crewAssignments, minYearsExperience):
    try:
        myCursor = myConn.cursor()
        sql = "SELECT promoteCrewMembersFunction(%s, %s)"
        myCursor.execute(sql, (crewAssignments, minYearsExperience))
    except:
        print("Call of promoteCrewMembers with arguments", crewAssignments, minYearsExperience, "had error", file=sys.stderr)
        myCursor.close()
        myConn.close()
        sys.exit(-1)

    row = myCursor.fetchone()
    myCursor.close()
    return(row[0])

#end promoteCrewMembers

def testDeparting(myConn):
    i = 0
    airport = ''
    while i < 5:
        if i == 0: airport = 'LAX'
        elif i == 1: airport = 'ATL'
        elif i == 2: airport = 'EWR'
        elif i == 3: airport = 'SFO'
        elif i == 4: airport = 'SJC'
        num = countNumberOfDepartingPassengers(myConn=myConn, departureAirport=airport)
        if num >= 0:
            print(f"Number of passengers for airport {airport} is {num}\n")
        elif num < 0:
            print(f"Error occurred. No departing passengers from {airport}\n")
        i+=1
# end printDeparting

def testUpdate(myConn):
    i = 0
    day = ''
    while i < 4:
        if i == 0: day = '2025-10-8'
        elif i == 1: day = '2025-10-9'
        elif i == 2: day = '2026-10-10'
        elif i == 3: day = '2025-10-10'
        num = updateReservationPayment(myConn=myConn, departureDate=day)
        if num < 0:
            print(f"Error. Invalid year: {day}\n")
        elif num >= 0:
            print(f"Number of Reservations whose paymentMethod values were updated by updateReservationPayment is {num}\n")
        i+=1
# end testUpdate

def testPromotion(myConn):
    i = 0
    assignments = 0
    years = 0
    while i < 5:
        if i == 0: 
            assignments = 4 
            years = 10
        elif i == 1: 
            assignments = 5
            years = 0
        elif i == 2:
            assignments = 0
            years = 0
        elif i == 3: 
            assignments = 2
            years = 5
        elif i == 4:
            assignments = 1
            years = 2
        num = promoteCrewMembers(myConn=myConn, crewAssignments=assignments, minYearsExperience=years)
        if num > 0:
            print(f"Number of promotions for crewAssignments {assignments} and minYearsExperience {years} is {num}\n")
        elif num < 0:
            print(f"Error. crewAssignments {assignments} + minYearsExperience {years} <= 0\n")
        i+=1

def main():
    port = "5432"
    userID = "cse180"
    pwd = "database4me"

    # Try to make a connection to the database
    try:
        myConn = psycopg2.connect(port=port, user=userID, password=pwd)
    except:
        print("Connection to database failed", file=sys.stderr)
        sys.exit(-1)
        
    # We're making every SQL statement a transaction that commits.
    # Don't need to explicitly begin a transaction.
    # Could have multiple statement in a transaction, using myConn.commit when we want to commit.
    
    myConn.autocommit = True
    
    # TESTS
    print("------TESTS FOR countNumberOfDepartingPassengers------")
    testDeparting(myConn=myConn)

    print("\n------TESTS FOR updateReservationPayment------")
    testUpdate(myConn=myConn)
    
    print("\n------TESTS FOR promoteCrewMembers------")
    testPromotion(myConn=myConn)
  
    myConn.close()
    sys.exit(0)
#end

#------------------------------------------------------------------------------
if __name__=='__main__':

    main()

# end
