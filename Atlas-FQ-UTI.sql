--1: Define Codesets
WITH Codesets AS (
    SELECT DISTINCT 0 AS codeset_id, concept_id
    FROM @montifiore.CONCEPT
    WHERE 0 = 1

    UNION ALL

    SELECT DISTINCT 1 AS codeset_id, concept_id
    FROM (
        SELECT DISTINCT concept_id
        FROM @montifiore.CONCEPT
        WHERE concept_id IN (1797513, 1743222, 19050750, 1742253, 1721543, 923081, 19027679, 40161662, 1592954, 35197938, 1789276, 1716721, 1747032, 1707800, 1716903, 36878831, 35198003, 35197897, 35198165, 1733765, 19041153, 43009030, 1712549)

        UNION

        SELECT DISTINCT c.concept_id
        FROM @montifiore.CONCEPT c
        JOIN @montifiore.CONCEPT_ANCESTOR ca
        ON c.concept_id = ca.descendant_concept_id
        WHERE ca.ancestor_concept_id IN (1797513, 1743222, 19050750, 1742253, 1721543, 923081, 19027679, 40161662, 1592954, 35197938, 1789276, 1716721, 1747032, 1707800, 1716903, 36878831, 35198003, 35197897, 35198165, 1733765, 19041153, 43009030, 1712549)
        AND c.invalid_reason IS NULL
    ) AS I
    LEFT JOIN (
        SELECT DISTINCT concept_id
        FROM @montifiore.CONCEPT
        WHERE concept_id IN (42479725, 40028359, 43258666, 40028361, 42629035, 42965658, 36269500, 40028718, 40028720, 40160496, 35605255, 40001157, 40066892, 40066893, 43695029, 40069651, 40069655, 40161667, 40059607, 40059318, 35144130, 40057467, 43678347, 35154779, 35141912, 42961482)

        UNION

        SELECT DISTINCT c.concept_id
        FROM @montifiore.CONCEPT c
        JOIN @montifiore.CONCEPT_ANCESTOR ca
        ON c.concept_id = ca.descendant_concept_id
        WHERE ca.ancestor_concept_id IN (42479725, 40028359, 43258666, 40028361, 42629035, 42965658, 36269500, 40028718, 40028720, 40160496, 35605255, 40001157, 40066892, 40066893, 43695029, 40069651, 40069655, 40161667, 40059607, 40059318, 35144130, 40057467, 43678347, 35154779, 35141912, 42961482)
        AND c.invalid_reason IS NULL
    ) AS E
    ON I.concept_id = E.concept_id
    WHERE E.concept_id IS NULL

    UNION ALL

    SELECT DISTINCT 2 AS codeset_id, concept_id
    FROM (
        SELECT DISTINCT concept_id
        FROM @montifiore.CONCEPT
        WHERE concept_id IN (81902)

        UNION

        SELECT DISTINCT c.concept_id
        FROM @montifiore.CONCEPT c
        JOIN @montifiore.CONCEPT_ANCESTOR ca
        ON c.concept_id = ca.descendant_concept_id
        WHERE ca.ancestor_concept_id IN (81902)
        AND c.invalid_reason IS NULL
    ) AS I
),

-- 2: Define Primary Events
PrimaryEvents AS (
    SELECT
        P.ordinal AS event_id,
        P.person_id,
        P.start_date,
        P.end_date,
        P.op_start_date,
        P.op_end_date,
        CAST(P.visit_occurrence_id AS BIGINT) AS visit_occurrence_id
    FROM (
        SELECT
            E.person_id,
            E.start_date,
            E.end_date,
            ROW_NUMBER() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) AS ordinal,
            OP.observation_period_start_date AS op_start_date,
            OP.observation_period_end_date AS op_end_date,
            CAST(E.visit_occurrence_id AS BIGINT) AS visit_occurrence_id
        FROM (
            SELECT
                de.person_id,
                de.drug_exposure_id AS event_id,
                de.drug_exposure_start_date AS start_date,
                COALESCE(de.drug_exposure_end_date, DATEADD(DAY, 1, de.drug_exposure_start_date)) AS end_date,
                de.drug_concept_id AS target_concept_id,
                de.visit_occurrence_id,
                de.drug_exposure_start_date AS sort_date
            FROM @montifiore.DRUG_EXPOSURE de
            JOIN Codesets cs
            ON de.drug_concept_id = cs.concept_id
            WHERE cs.codeset_id = 1
        ) AS E
        JOIN @montifiore.OBSERVATION_PERIOD OP
        ON E.person_id = OP.person_id
        AND E.start_date BETWEEN OP.observation_period_start_date AND OP.observation_period_end_date
    ) AS P
    WHERE P.ordinal = 1
),

-- 3: Define Inclusion Events
InclusionEvents AS (
    SELECT DISTINCT
        0 AS inclusion_rule_id,
        Q.person_id,
        Q.event_id
    FROM PrimaryEvents Q
    JOIN (
        SELECT DISTINCT
            P.person_id,
            P.event_id
        FROM PrimaryEvents P
        JOIN (
            SELECT
                C.person_id,
                C.condition_occurrence_id AS event_id,
                C.condition_start_date AS start_date,
                COALESCE(C.condition_end_date, DATEADD(DAY, 1, C.condition_start_date)) AS end_date,
                C.condition_concept_id AS target_concept_id,
                C.visit_occurrence_id,
                C.condition_start_date AS sort_date
            FROM @montifiore.CONDITION_OCCURRENCE C
            JOIN Codesets cs
            ON C.condition_concept_id = cs.concept_id
            WHERE cs.codeset_id = 2
        ) AS A
        ON A.person_id = P.person_id
        AND A.start_date BETWEEN DATEADD(DAY, -30, P.start_date) AND DATEADD(DAY, 30, P.start_date)
        GROUP BY P.person_id, P.event_id
        HAVING COUNT(A.target_concept_id) >= 1
    ) AS CQ
    ON Q.person_id = CQ.person_id
    AND Q.event_id = CQ.event_id
),

-- 4: Define Included Events
IncludedEvents AS (
    SELECT DISTINCT
        Q.event_id,
        Q.person_id,
        Q.start_date,
        Q.end_date,
        Q.op_start_date,
        Q.op_end_date
    FROM PrimaryEvents Q
    JOIN InclusionEvents I
    ON Q.person_id = I.person_id
    AND Q.event_id = I.event_id
),

--5: Define Drug Target
DrugTarget AS (
    SELECT DISTINCT
        de.person_id,
        de.drug_exposure_start_date,
        COALESCE(de.drug_exposure_end_date, DATEADD(DAY, de.days_supply, de.drug_exposure_start_date), DATEADD(DAY, 1, de.drug_exposure_start_date)) AS drug_exposure_end_date
    FROM @montifiore.DRUG_EXPOSURE de
    JOIN IncludedEvents IE
    ON de.person_id = IE.person_id
    JOIN Codesets cs
    ON de.drug_concept_id = cs.concept_id
    WHERE cs.codeset_id = 1

    UNION ALL

    SELECT DISTINCT
        de.person_id,
        de.drug_exposure_start_date,
        COALESCE(de.drug_exposure_end_date, DATEADD(DAY, de.days_supply, de.drug_exposure_start_date), DATEADD(DAY, 1, de.drug_exposure_start_date)) AS drug_exposure_end_date
    FROM @montifiore.DRUG_EXPOSURE de
    JOIN IncludedEvents IE
    ON de.person_id = IE.person_id
    JOIN Codesets cs
    ON de.drug_source_concept_id = cs.concept_id
    WHERE cs.codeset_id = 1
),

--6: Define Strategy Ends
StrategyEnds AS (
    SELECT DISTINCT
        IE.event_id,
        IE.person_id,
        MIN(de.drug_exposure_start_date) AS era_start_date,
        DATEADD(DAY, 0, MIN(de.drug_exposure_end_date)) AS era_end_date
    FROM DrugTarget de
    JOIN IncludedEvents IE
    ON de.person_id = IE.person_id
    AND de.drug_exposure_start_date BETWEEN IE.start_date AND IE.end_date
    GROUP BY IE.event_id, IE.person_id
),

--7: Final Cohort
FinalCohort AS (
    SELECT DISTINCT
        IE.person_id,
        MIN(IE.start_date) AS cohort_start_date,
        SE.era_end_date AS cohort_end_date
    FROM IncludedEvents IE
    JOIN StrategyEnds SE
    ON IE.event_id = SE.event_id
    AND IE.person_id = SE.person_id
    GROUP BY IE.person_id, SE.era_end_date
)

--8: Retrieve Final Cohort
SELECT DISTINCT
    @target_cohort_id AS cohort_definition_id,
    FC.person_id AS subject_id,
    FC.cohort_start_date,
    FC.cohort_end_date
FROM FinalCohort FC;
