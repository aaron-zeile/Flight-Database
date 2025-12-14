CREATE FUNCTION promoteCrewMembersFunction(crew INT, years INT)
RETURNS INT AS $$
DECLARE
    numPromoted INT;
BEGIN
    IF (crew + years) <= 0 THEN
        RETURN -1;
    END IF;
    IF crew < 0 OR years < 0 THEN
        RETURN -1;
    END IF;

    UPDATE CrewMember c
    SET crewRole = 'Senior ' || c.crewRole 
    WHERE c.yearsExperience >= years AND 
          c.crewID IN (
            SELECT crewID
            FROM FlightCrewAssignment
            GROUP BY crewID 
            HAVING COUNT(*) >= crew
          )
          AND c.crewRole NOT LIKE 'Senior %';
    
    GET DIAGNOSTICS numPromoted = ROW_COUNT;

    RETURN numPromoted;

END;
$$ LANGUAGE plpgsql