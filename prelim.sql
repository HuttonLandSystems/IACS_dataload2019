
/*--*2019 Data Load
1. Create temp tables, remove select rows, add select columns
        dropped columns: payment_region, organic_status
        dropped data: all year 2019 data, ((land_use = 'EXCL' OR land_use = 'DELETED_LANDUSE') AND (land_use_area = 0 OR land_use_area IS NULL)), 
                      duplicate records from payment_region, NULL hapar_id and land_use = ' '
        recast claim_id column to accept multiple values
        rename all claim_id_p/s so no problems with unique ids

2. Fix land_parcel_area IS NULL/0
        infer land_parcel_area from same hapar_id
        infer land_parcel_area from land_use_area in same row
        delete where land_parcel_area IS NULL/0 AND land_use_area = 0

3. Fix land_use_area IS NULL or 0
        copy land_parcel_area for single claims where land_parcel_area = bps_eligible_area
        update NULL land_use_areas with inferred values from other years
        removes records "Waiting for deadline/inspection" AND (bps_eligible_area = 0 and percent = 0)
        infer land_use_area from bps_claimed_area where 0 
        delete land_use_area IS NULL 

4. Find renter records in wrong tables ( sum(land_use_area) > land_parcel_area )
        finds multiple businesses claiming on same land in permanent table and marks them as seasonal, and vice versa
        mark owners in seasonal table by LLO flag 
        move marked records to respective tables
            
5. Combine mutually exclusive       
        move mutually exclusive hapar_ids to separate table
        separate claims on parcels (which exists in both p and s sheets but) which are only claimed by one party for one year

6. Joins 
        first join on hapar_id, year, land_use, land_use_area
            delete from original table where join above
        second join on hapar_id, year, land_use 
            delete from original table where join above 
        third join on hapar_id, year 
            delete from original table where join above
*/

--*Step 1. Create temp tables, remove select rows, add select columns
-- DROPPED COLUMNS: payment_region, organic_status
-- DROPPED DATA: all year 2019 data, ((land_use = 'EXCL' OR land_use = 'DELETED_LANDUSE') AND (land_use_area = 0 OR land_use_area IS NULL)), 
--               duplicate records from payment_region, NULL hapar_id and land_use = ' '

--1,926,682 in all perm temp
DROP TABLE IF EXISTS temp_permanent CASCADE;
CREATE TEMP TABLE temp_permanent AS 
WITH subq AS
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            land_parcel_area,
            verified_exclusion,
            bps_eligible_area,
            land_activity,
            organic_status,
            land_use,
            land_use_area,
            land_leased_out,
            lfass,
            bps_claimed_area,
            application_status,
            payment_region,
            is_perm_flag,
            year,
            claim_id_p,
            ROW_NUMBER () OVER (PARTITION BY mlc_hahol_id,
                                             habus_id,
                                             hahol_id,
                                             hapar_id,
                                             land_parcel_area,
                                             verified_exclusion,
                                             bps_eligible_area,
                                             land_activity,
                                             organic_status,
                                             land_use,
                                             land_use_area,
                                             lfass,
                                             bps_claimed_area,
                                             application_status,
                                             is_perm_flag,
                                             year
                                ORDER BY mlc_hahol_id,
                                         habus_id,
                                         hahol_id,
                                         hapar_id,
                                         land_parcel_area,
                                         verified_exclusion,
                                         bps_eligible_area,
                                         land_activity,
                                         organic_status,
                                         land_use,
                                         land_use_area,
                                         lfass,
                                         bps_claimed_area,
                                         application_status,
                                         is_perm_flag,
                                         year) row_num
     FROM rpid.saf_permanent_land_parcels_deliv20190911)
SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       land_parcel_area,
       ABS(bps_eligible_area) AS bps_eligible_area, -- fixes 11 rows
       bps_claimed_area,
       verified_exclusion,
       ABS(land_use_area) AS land_use_area, -- fixes 2 rows
       land_use,
       land_activity,
       application_status,
       land_leased_out,
       lfass AS lfass_flag,
       is_perm_flag,
       claim_id_p,
       YEAR
FROM subq
WHERE row_num < 2 -- removes 55,249 rows
    AND hapar_id IS NOT NULL -- removes 609 rows
    AND land_use <> '' -- removes 1,761 rows
    AND year <> 2019; -- removes 681,711 rows

DELETE
FROM temp_permanent
WHERE ((land_use = 'EXCL'
        OR land_use = 'DELETED_LANDUSE')
       AND (land_use_area = 0
            OR land_use_area IS NULL)); -- removes 748,000 rows

ALTER TABLE temp_permanent ADD change_note VARCHAR;

--175,489 in all seas temp
DROP TABLE IF EXISTS temp_seasonal CASCADE;
CREATE TEMP TABLE temp_seasonal AS 
WITH subq AS
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            land_parcel_area,
            verified_exclusion,
            bps_eligible_area,
            land_activity,
            organic_status,
            land_use,
            land_use_area,
            land_leased_out,
            lfass,
            bps_claimed_area,
            application_status,
            payment_region,
            is_perm_flag,
            year,
            claim_id_s,
            ROW_NUMBER () OVER (PARTITION BY mlc_hahol_id,
                                             habus_id,
                                             hahol_id,
                                             hapar_id,
                                             land_parcel_area,
                                             verified_exclusion,
                                             bps_eligible_area,
                                             land_activity,
                                             organic_status,
                                             land_use,
                                             land_use_area,
                                             land_leased_out,
                                             lfass,
                                             bps_claimed_area,
                                             application_status,
                                             is_perm_flag,
                                             year
                                ORDER BY mlc_hahol_id,
                                         habus_id,
                                         hahol_id,
                                         hapar_id,
                                         land_parcel_area,
                                         verified_exclusion,
                                         bps_eligible_area,
                                         land_activity,
                                         organic_status,
                                         land_use,
                                         land_use_area,
                                         land_leased_out,
                                         lfass,
                                         bps_claimed_area,
                                         application_status,
                                         is_perm_flag,
                                         year) row_num
     FROM rpid.saf_seasonal_land_parcels_deliv20190911)
SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       land_parcel_area,
       ABS(bps_eligible_area) AS bps_eligible_area, -- fixes 1 row
       bps_claimed_area,
       verified_exclusion,
       ABS(land_use_area) AS land_use_area, -- fixes 0 rows
       land_use,
       land_activity,
       application_status,
       land_leased_out,
       lfass AS lfass_flag,
       is_perm_flag,
       claim_id_s,
       YEAR
FROM subq
WHERE row_num < 2 -- removes 4,362 rows
    AND hapar_id IS NOT NULL -- removes 7 rows
    AND land_use <> '' -- removes 3 rows
    AND year <> 2019; -- removes 60,825 rows

DELETE
FROM temp_seasonal
WHERE ((land_use = 'EXCL'
        OR land_use = 'DELETED_LANDUSE')
       AND (land_use_area = 0
            OR land_use_area IS NULL)); -- removes 79,016 rows

ALTER TABLE temp_seasonal ADD change_note VARCHAR;

--recast claim_id column to accept multiple values
ALTER TABLE temp_permanent 
ALTER COLUMN claim_id_p TYPE VARCHAR;

ALTER TABLE temp_seasonal
ALTER COLUMN claim_id_s TYPE VARCHAR;

--rename claim_id_p/s so no problems with unique ids between tables
UPDATE temp_permanent 
SET claim_id_p = 'P' || claim_id_p
WHERE claim_id_p NOT LIKE '%P%';

UPDATE temp_seasonal 
SET claim_id_s = 'S' || claim_id_s
WHERE claim_id_s NOT LIKE '%S%';

--*Step 2. Fix land_parcel_area IS NULL or 0
--infer land_parcel_area where same hapar_id
UPDATE temp_permanent AS t
SET land_parcel_area = sub.land_parcel_area,
    change_note = CONCAT(t.change_note, 'land_parcel_area inferred from same hapar_id; ')
FROM
    (SELECT *
     FROM temp_permanent
     WHERE land_parcel_area IS NOT NULL
         OR land_parcel_area <> 0) sub
WHERE t.hapar_id = sub.hapar_id
    AND t.land_parcel_area IS NULL; -- updates 3 rows

UPDATE temp_seasonal AS t
SET land_parcel_area = sub.land_parcel_area,
    change_note = CONCAT(t.change_note, 'land_parcel_area inferred from same hapar_id; ')
FROM
    (SELECT *
     FROM temp_seasonal
     WHERE land_parcel_area IS NOT NULL
         OR land_parcel_area <> 0) sub
WHERE t.hapar_id = sub.hapar_id
    AND t.land_parcel_area IS NULL; -- updates 5 rows

--infer land_parcel_area from land_use_area in same row
UPDATE temp_permanent
SET land_parcel_area = land_use_area,
    change_note = CONCAT(change_note, 'land_parcel_area inferred from land_use_area in single claim row; ')
WHERE land_parcel_area IS NULL; -- updates 13 rows

UPDATE temp_seasonal
SET land_parcel_area = land_use_area,
    change_note = CONCAT(change_note, 'land_parcel_area inferred from land_use_area in single claim row; ')
WHERE land_parcel_area IS NULL; -- updates 1 rows

--delete where land_parcel_area IS NULL/0 AND land_use_area = 0
DELETE
FROM temp_permanent
WHERE (land_parcel_area IS NULL
       AND land_use_area = 0)
    OR (land_parcel_area = 0
        AND land_use_area =0); -- removes 19 rows

DELETE
FROM temp_seasonal
WHERE (land_parcel_area IS NULL
       AND land_use_area = 0)
    OR (land_parcel_area = 0
        AND land_use_area =0); -- removes 1 rows

--*Step 3. Fix land_use_area IS NULL or 0 
--Permanent: 351 NULL,   5,252 zero
--Seasonal:  898 NULL,   420 zero
--copy land_parcel_area for single claims where land_parcel_area = bps_eligible_area
WITH sub AS
    (SELECT hapar_id,
            year
     FROM
         (SELECT hapar_id,
                 year,
                 COUNT(land_use) as lu_count,
                 SUM(land_use_area) as sum_lu
          FROM temp_permanent AS tp
          GROUP BY hapar_id,
                   year) foo
     WHERE lu_count = 1
         AND (sum_lu = 0 OR sum_lu IS NULL))
UPDATE temp_permanent
SET land_use_area = p.land_parcel_area,
    change_note = CONCAT(p.change_note, 'land_use_area inferred where land_parcel_area = bps_eligible_area for single claims; ')
FROM sub
JOIN temp_permanent AS p USING (hapar_id,
                                year)
WHERE temp_permanent.hapar_id = sub.hapar_id
    AND temp_permanent.year = sub.year
    AND temp_permanent.land_parcel_area = temp_permanent.bps_eligible_area; -- updates 715 rows

WITH sub AS
    (SELECT hapar_id,
            year
     FROM
         (SELECT hapar_id,
                 year,
                 COUNT(land_use) as lu_count,
                 SUM(land_use_area) as sum_lu
          FROM temp_seasonal AS ts
          GROUP BY hapar_id,
                   year) foo
     WHERE lu_count = 1
         AND (sum_lu = 0 OR sum_lu IS NULL))
UPDATE temp_seasonal
SET land_use_area = s.land_parcel_area,
    change_note = CONCAT(s.change_note, 'land_use_area inferred where land_parcel_area = bps_eligible_area for single claims; ')
FROM sub
JOIN temp_seasonal AS s USING (hapar_id,
                                year)
WHERE temp_seasonal.hapar_id = sub.hapar_id
    AND temp_seasonal.year = sub.year
    AND temp_seasonal.land_parcel_area = temp_seasonal.bps_eligible_area; -- updates 850 rows 

-- update NULL land_use_areas with inferred values from other years
WITH sub1 AS
    (SELECT *
     FROM
         (SELECT hapar_id,
                 sum(land_use_area) OVER(PARTITION BY hapar_id, year) AS sum_lua,
                 land_use,
                 year
          FROM temp_permanent) foo
     WHERE sum_lua IS NULL),
     sub2 AS
    (SELECT hapar_id,
            land_use,
            p.land_use_area AS fix_lu
     FROM sub1
     JOIN temp_permanent AS p USING (hapar_id,
                                     land_use)
     WHERE p.land_use_area IS NOT NULL
     GROUP BY hapar_id,
              land_use,
              p.land_use_area
     ORDER BY hapar_id)
UPDATE temp_permanent AS p
SET land_use_area = fix_lu,
    change_note = CONCAT(p.change_note, 'land_use_area inferred from other year where same land_use; ')
FROM temp_permanent
JOIN sub2 USING (hapar_id,
                 land_use)
WHERE p.hapar_id = sub2.hapar_id
    AND p.land_use = sub2.land_use
    AND p.land_use_area IS NULL; -- updates 77 rows

WITH sub1 AS
    (SELECT *
     FROM
         (SELECT hapar_id,
                 sum(land_use_area) OVER(PARTITION BY hapar_id, year) AS sum_lua,
                 land_use,
                 year
          FROM temp_seasonal) foo
     WHERE sum_lua IS NULL),
     sub2 AS
    (SELECT hapar_id,
            land_use,
            s.land_use_area AS fix_lu
     FROM sub1
     JOIN temp_seasonal AS s USING (hapar_id,
                                    land_use)
     WHERE s.land_use_area IS NOT NULL
     GROUP BY hapar_id,
              land_use,
              s.land_use_area
     ORDER BY hapar_id)
UPDATE temp_seasonal AS s
SET land_use_area = fix_lu,
    change_note = CONCAT(s.change_note, 'land_use_area inferred from other year where same land_use; ')
FROM temp_seasonal
JOIN sub2 USING (hapar_id,
                 land_use)
WHERE s.hapar_id = sub2.hapar_id
    AND s.land_use = sub2.land_use
    AND s.land_use_area IS NULL; -- updates 114 rows 

--fix land_use_area <> bps_claimed_area
UPDATE temp_permanent
SET land_use_area = bps_claimed_area,
    change_note = CONCAT(change_note, 'adjust land_use_area to match bps_claimed_area; ')
WHERE bps_claimed_area <> land_use_area
    AND bps_claimed_area <> 0; -- updates 187,593 rows

UPDATE temp_seasonal
SET land_use_area = bps_claimed_area,
    change_note = CONCAT(change_note, 'adjust land_use_area to match bps_claimed_area; ')
WHERE bps_claimed_area <> land_use_area
    AND bps_claimed_area <> 0; -- updates 21,603 rows

--removes records "Waiting for deadline/inspection" AND (bps_eligible_area = 0 and percent = 0)
WITH wait_status AS
    (SELECT hapar_id,
            land_parcel_area,
            year,
            sum_lua,
            percent,
            application_status,
            bps_eligible_area,
            land_use_area
     FROM
         (SELECT hapar_id,
                 land_parcel_area,
                 land_use_area,
                 land_use,
                 year,
                 application_status,
                 bps_eligible_area,
                 SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) AS sum_lua,
                 (SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) / land_parcel_area) * 100 AS percent
          FROM temp_permanent) foo
     WHERE (sum_lua = 0
            or sum_lua IS NULL)
         AND application_status LIKE '%Wait%'
         OR (bps_eligible_area = 0
             AND percent = 0))
DELETE
FROM temp_permanent USING wait_status
WHERE temp_permanent.hapar_id = wait_status.hapar_id
    AND temp_permanent.year = wait_status.year; -- removes 26 rows

WITH wait_status AS
    (SELECT hapar_id,
            land_parcel_area,
            year,
            sum_lua,
            percent,
            application_status,
            bps_eligible_area,
            land_use_area
     FROM
         (SELECT hapar_id,
                 land_parcel_area,
                 land_use_area,
                 land_use,
                 year,
                 application_status,
                 bps_eligible_area,
                 SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) AS sum_lua,
                 (SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) / land_parcel_area) * 100 AS percent
          FROM temp_seasonal) foo
     WHERE (sum_lua = 0
            or sum_lua IS NULL)
         AND application_status LIKE '%Wait%'
         OR (bps_eligible_area = 0
             AND percent = 0))
DELETE
FROM temp_seasonal USING wait_status
WHERE temp_seasonal.hapar_id = wait_status.hapar_id
    AND temp_seasonal.year = wait_status.year; -- removes 9 rows

/*--infer land_use_area from bps_claimed_area where 0 
UPDATE temp_permanent
SET land_use_area = bps_claimed_area,
    change_note = CONCAT(change_note, 'land_use_area inferred from bps_claimed_area; ')
WHERE (land_use_area = 0
       OR land_use_area IS NULL)
    AND bps_claimed_area <> 0; --updates 11 rows

UPDATE temp_seasonal
SET land_use_area = bps_claimed_area,
    change_note = CONCAT(change_note, 'land_use_area inferred from bps_claimed_area; ')
WHERE (land_use_area = 0
       OR land_use_area IS NULL)
    AND bps_claimed_area <> 0; --update 1 rows*/


--set remaining NULL land_use_area to 0
UPDATE temp_permanent 
SET land_use_area = 0 
WHERE land_use_area IS NULL; --updates 353 rows 

UPDATE temp_seasonal
SET land_use_area = 0 
WHERE land_use_area IS NULL; --updates 1,187 rows



--*STEP 4. Find renter records in wrong tables 
--finds multiple businesses claiming on same land in same table and marks them as either owner/renter
WITH mult_busses AS
    (SELECT *
     FROM
         (SELECT mlc_hahol_id,
                 habus_id,
                 hahol_id,
                 hapar_id,
                 YEAR,
                 ROW_NUMBER () OVER (PARTITION BY hapar_id,
                                                  YEAR)
          FROM temp_permanent
          GROUP BY mlc_hahol_id,
                   habus_id,
                   hahol_id,
                   hapar_id,
                   YEAR) foo
     WHERE ROW_NUMBER > 1),
     bps_claim AS
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            year,
            SUM(bps_claimed_area) AS sum_bps
     FROM temp_permanent
     GROUP BY mlc_hahol_id,
              habus_id,
              hahol_id,
              hapar_id,
              year)
UPDATE temp_permanent AS t
SET claim_id_p = 'S' || TRIM('P'
                             FROM t.claim_id_p) || '-01',
    is_perm_flag = 'N',
    change_note = CONCAT(t.change_note, 'S record moved from permanent to seasonal sheet; ')
FROM temp_permanent AS a
JOIN mult_busses USING (mlc_hahol_id,
                        habus_id,
                        hahol_id,
                        hapar_id,
                        year)
JOIN bps_claim USING (mlc_hahol_id,
                      habus_id,
                      hahol_id,
                      hapar_id,
                      year)
WHERE sum_bps <> 0
    AND t.land_leased_out = 'N'
    AND t.mlc_hahol_id = a.mlc_hahol_id
    AND t.habus_id = a.habus_id
    AND t.hahol_id = a.hahol_id
    AND t.hapar_id = a.hapar_id
    AND t.year = a.year; --updates 46 rows

WITH mult_busses AS
    (SELECT *
     FROM
         (SELECT mlc_hahol_id,
                 habus_id,
                 hahol_id,
                 hapar_id,
                 YEAR,
                 ROW_NUMBER () OVER (PARTITION BY hapar_id,
                                                  YEAR)
          FROM temp_seasonal
          GROUP BY mlc_hahol_id,
                   habus_id,
                   hahol_id,
                   hapar_id,
                   YEAR) foo
     WHERE ROW_NUMBER > 1),
     bps_claim AS
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            year,
            SUM(bps_claimed_area) AS sum_bps
     FROM temp_seasonal
     GROUP BY mlc_hahol_id,
              habus_id,
              hahol_id,
              hapar_id,
              year)
UPDATE temp_seasonal AS t
SET land_leased_out = (CASE
                           WHEN t.land_use <> 'EXCL' THEN 'Y'
                           ELSE t.land_leased_out
                       END),
    claim_id_s = 'P' || TRIM('S'
                                  from t.claim_id_s) || '-01',
    is_perm_flag = 'Y',
    change_note = CONCAT(t.change_note, 'P record moved from seasonal to permanent sheet; ')
FROM temp_seasonal AS a
JOIN mult_busses USING (mlc_hahol_id,
                        habus_id,
                        hahol_id,
                        hapar_id,
                        year)
JOIN bps_claim USING (mlc_hahol_id,
                      habus_id,
                      hahol_id,
                      hapar_id,
                      year)
WHERE sum_bps = 0
    AND t.mlc_hahol_id = a.mlc_hahol_id
    AND t.habus_id = a.habus_id
    AND t.hahol_id = a.hahol_id
    AND t.hapar_id = a.hapar_id
    AND t.year = a.year; --updates 1,410 rows

--moves marked records to respective tables
INSERT INTO temp_permanent 
SELECT * 
FROM temp_seasonal 
WHERE claim_id_s LIKE '%P%';
DELETE FROM temp_seasonal 
WHERE claim_id_s LIKE '%P%'; --moves 1,410 rows

INSERT INTO temp_seasonal 
SELECT * 
FROM temp_permanent 
WHERE claim_id_p LIKE '%S%';
DELETE FROM temp_permanent 
WHERE claim_id_p LIKE '%S%'; -- moves 46 rows

--finds swapped owner/renters (owners in seasonal table and renters in permanent table that join on hapar_id, year, land_use, land_use_area) 
WITH find_switches AS
    (SELECT hapar_id,
            YEAR
     FROM
         (SELECT hapar_id,
                 YEAR,
                 SUM(owner_bps_claimed_area) AS owner_bps,
                 SUM(user_bps_claimed_area) AS user_bps
          FROM
              (SELECT hapar_id,
                      p.bps_claimed_area AS owner_bps_claimed_area,
                      s.bps_claimed_area AS user_bps_claimed_area,
                      year
               FROM temp_permanent AS p
               JOIN temp_seasonal AS s USING (hapar_id,
                                              year,
                                              land_use,
                                              land_use_area)) foo
          GROUP BY hapar_id,
                   YEAR) foo2
     WHERE owner_bps <> 0
         AND user_bps = 0)
UPDATE temp_permanent AS t
SET land_leased_out = (CASE
                           WHEN t.land_use <> 'EXCL' THEN 'Y'
                           ELSE t.land_leased_out
                       END),
    claim_id_p = 'S' || TRIM('P'
                             FROM t.claim_id_p) || '-01',
    is_perm_flag = 'N',
    change_note = CONCAT(t.change_note, 'S record moved from permanent to seasonal sheet; ')
FROM find_switches AS a
JOIN temp_permanent AS b USING (hapar_id,
                                year)
WHERE t.hapar_id = a.hapar_id
    AND t.land_leased_out = 'N'
    AND t.year = a.year; --updates 5,104 rows

WITH find_switches AS
    (SELECT hapar_id,
            YEAR
     FROM
         (SELECT hapar_id,
                 YEAR,
                 sum(owner_bps_claimed_area) AS owner_bps,
                 sum(user_bps_claimed_area) AS user_bps
          FROM
              (SELECT hapar_id,
                      p.bps_claimed_area AS owner_bps_claimed_area,
                      s.bps_claimed_area AS user_bps_claimed_area,
                      year
               FROM temp_permanent AS p
               JOIN temp_seasonal AS s USING (hapar_id,
                                              year,
                                              land_use,
                                              land_use_area)) foo
          GROUP BY hapar_id,
                   YEAR) foo2
     WHERE owner_bps <> 0
         AND user_bps = 0)
UPDATE temp_seasonal AS t
SET land_leased_out = (CASE
                           WHEN t.land_use <> 'EXCL' THEN 'Y'
                           ELSE t.land_leased_out
                       END),
    claim_id_s = 'P' || TRIM('S'
                             from t.claim_id_s) || '-01',
    is_perm_flag = 'Y',
    change_note = CONCAT(t.change_note, 'P record moved from seasonal to permanent sheet; ')
FROM find_switches AS a
JOIN temp_seasonal AS b USING (hapar_id,
                               year)
WHERE t.hapar_id = a.hapar_id
    AND t.year = a.year; -- updates 5,106 rows  

--moves marked records to respective tables
INSERT INTO temp_permanent 
SELECT * 
FROM temp_seasonal 
WHERE claim_id_s LIKE '%P%';
DELETE FROM temp_seasonal 
WHERE claim_id_s LIKE '%P%'; --moves 5,106 rows

INSERT INTO temp_seasonal 
SELECT * 
FROM temp_permanent 
WHERE claim_id_p LIKE '%S%';
DELETE FROM temp_permanent 
WHERE claim_id_p LIKE '%S%'; -- moves 5,104 rows 

--*STEP 5. Combine mutually exclusive
--move mutually exclusive hapar_ids to separate table 
DROP TABLE IF EXISTS combine; 
SELECT mlc_hahol_id AS owner_mlc_hahol_id,
       NULL :: BIGINT AS user_mlc_hahol_id,
       habus_id AS owner_habus_id,
       NULL :: BIGINT AS user_habus_id,
       hahol_id AS owner_hahol_id,
       NULL :: BIGINT AS user_hahol_id,
       hapar_id,
       land_parcel_area AS owner_land_parcel_area,
       NULL :: BIGINT AS user_land_parcel_area,
       bps_eligible_area AS owner_bps_eligible_area,
       NULL :: BIGINT AS user_bps_eligible_area,
       bps_claimed_area AS owner_bps_claimed_area,
       NULL :: BIGINT AS user_bps_claimed_area,
       verified_exclusion AS owner_verified_exclusion,
       NULL :: BIGINT AS user_verified_exclusion,
       land_use_area AS owner_land_use_area,
       NULL :: BIGINT AS user_land_use_area,
       land_use AS owner_land_use,
       NULL :: VARCHAR AS user_land_use,
       land_activity AS owner_land_activity,
       NULL :: VARCHAR AS user_land_activity,
       application_status AS owner_application_status,
       NULL :: VARCHAR AS user_application_status,
       land_leased_out,
       lfass_flag AS owner_lfass_flag,
       NULL :: VARCHAR AS user_lfass_flag,
       claim_id_p AS claim_id,
       year,
       change_note INTO TEMP TABLE combine
FROM temp_permanent
WHERE hapar_id NOT IN
        (SELECT DISTINCT hapar_id
         FROM temp_seasonal); 
DELETE
FROM temp_permanent AS t USING combine
WHERE t.hapar_id = combine.hapar_id; --moves 1,826,393 rows

INSERT INTO combine 
SELECT NULL :: BIGINT AS owner_mlc_hahol_id,
       mlc_hahol_id AS user_mlc_hahol_id,
       NULL :: BIGINT AS owner_habus_id,
       habus_id AS user_habus_id,
       NULL :: BIGINT AS owner_hahol_id,
       hahol_id AS user_hahol_id,
       hapar_id,
       NULL :: BIGINT AS owner_land_parcel_area, 
       land_parcel_area AS user_land_parcel_area,
       NULL :: BIGINT AS owner_bps_eligible_area,
       bps_eligible_area AS user_bps_eligible_area,
       NULL :: BIGINT AS owner_bps_claimed_area,
       bps_claimed_area AS user_bps_claimed_area,
       NULL :: BIGINT AS owner_verified_exclusion,
       verified_exclusion AS user_verified_exclusion,
       NULL :: BIGINT AS owner_land_use_area,
       land_use_area AS user_land_use_area,
       NULL :: VARCHAR AS owner_land_use,
       land_use AS user_land_use,
       NULL :: VARCHAR AS owner_land_activity,
       land_activity AS user_land_activity,
       NULL :: VARCHAR AS owner_application_status,
       application_status AS user_application_status,
       land_leased_out,
       NULL :: VARCHAR AS owner_lfass_flag,
       lfass_flag AS user_lfass_flag,
       claim_id_s AS claim_id,
       year,
       change_note
FROM temp_seasonal 
WHERE hapar_id NOT IN
        (SELECT DISTINCT hapar_id
         FROM temp_permanent); 
DELETE
FROM temp_seasonal AS t USING combine
WHERE t.hapar_id = combine.hapar_id; --move 94,582 rows

--*Step 6. Join
--first join on hapar_id, year, land_use, land_use_area
DROP TABLE IF EXISTS joined; 
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       p.land_parcel_area AS owner_land_parcel_area,
       s.land_parcel_area AS user_land_parcel_area,
       p.bps_eligible_area AS owner_bps_eligible_area,
       s.bps_eligible_area AS user_bps_eligible_area,
       p.bps_claimed_area AS owner_bps_claimed_area,
       s.bps_claimed_area AS user_bps_claimed_area,
       p.verified_exclusion AS owner_verified_exclusion,
       s.verified_exclusion AS user_verified_exclusion,
       p.land_use_area AS owner_land_use_area,
       s.land_use_area AS user_land_use_area,
       p.land_use AS owner_land_use,
       s.land_use AS user_land_use,
       p.land_activity AS owner_land_activity,
       s.land_activity AS user_land_activity,
       p.application_status AS owner_application_status,
       s.application_status AS user_application_status,
       'Y' AS land_leased_out,
       p.lfass_flag AS owner_lfass_flag,
       s.lfass_flag AS user_lfass_flag,
       CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
       year,
       CASE
           WHEN p.change_note IS NOT NULL
                AND s.change_note IS NOT NULL THEN CONCAT(p.change_note, s.change_note, 'first join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NOT NULL THEN CONCAT(s.change_note, 'first join; ')
           WHEN s.change_note IS NULL
                AND p.change_note IS NOT NULL THEN CONCAT(p.change_note, 'first join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NULL THEN 'first join; '
       END AS change_note INTO TEMP TABLE joined
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hapar_id,
                               year,
                               land_use,
                               land_use_area); --40,124 rows

--delete from original table where join above
WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_permanent AS t USING joined_ids AS a  
WHERE t.claim_id_p = a.claim_id_p; -- 39,852 rows 

WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_seasonal AS t USING joined_ids AS a  
WHERE t.claim_id_s = a.claim_id_s; --39,690 rows 

--second join on hapar_id, year, land_use 
INSERT INTO joined 
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       p.land_parcel_area AS owner_land_parcel_area,
       s.land_parcel_area AS user_land_parcel_area,
       p.bps_eligible_area AS owner_bps_eligible_area,
       s.bps_eligible_area AS user_bps_eligible_area,
       p.bps_claimed_area AS owner_bps_claimed_area,
       s.bps_claimed_area AS user_bps_claimed_area,
       p.verified_exclusion AS owner_verified_exclusion,
       s.verified_exclusion AS user_verified_exclusion,
       p.land_use_area AS owner_land_use_area,
       s.land_use_area AS user_land_use_area,
       p.land_use AS owner_land_use,
       s.land_use AS user_land_use,
       p.land_activity AS owner_land_activity,
       s.land_activity AS user_land_activity,
       p.application_status AS owner_application_status,
       s.application_status AS user_application_status,
       p.land_leased_out,
       p.lfass_flag AS owner_lfass_flag,
       s.lfass_flag AS user_lfass_flag,
       CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
       year,
       CASE
           WHEN p.change_note IS NOT NULL
                AND s.change_note IS NOT NULL THEN CONCAT(p.change_note, s.change_note, 'second join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NOT NULL THEN CONCAT(s.change_note, 'second join; ')
           WHEN s.change_note IS NULL
                AND p.change_note IS NOT NULL THEN CONCAT(p.change_note, 'second join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NULL THEN 'second join; '
       END AS change_note 
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hapar_id,
                               year,
                               land_use); --13,758 rows

--delete from original table where join above
WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_permanent AS t USING joined_ids AS a  
WHERE t.claim_id_p = a.claim_id_p; -- 13,467 rows 

WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_seasonal AS t USING joined_ids AS a  
WHERE t.claim_id_s = a.claim_id_s; --13,468 rows 

--third join on hapar_id, year, land_use_area
INSERT INTO joined 
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       p.land_parcel_area AS owner_land_parcel_area,
       s.land_parcel_area AS user_land_parcel_area,
       p.bps_eligible_area AS owner_bps_eligible_area,
       s.bps_eligible_area AS user_bps_eligible_area,
       p.bps_claimed_area AS owner_bps_claimed_area,
       s.bps_claimed_area AS user_bps_claimed_area,
       p.verified_exclusion AS owner_verified_exclusion,
       s.verified_exclusion AS user_verified_exclusion,
       p.land_use_area AS owner_land_use_area,
       s.land_use_area AS user_land_use_area,
       p.land_use AS owner_land_use,
       s.land_use AS user_land_use,
       p.land_activity AS owner_land_activity,
       s.land_activity AS user_land_activity,
       p.application_status AS owner_application_status,
       s.application_status AS user_application_status,
       p.land_leased_out,
       p.lfass_flag AS owner_lfass_flag,
       s.lfass_flag AS user_lfass_flag,
       CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
       year,
       CASE
           WHEN p.change_note IS NOT NULL
                AND s.change_note IS NOT NULL THEN CONCAT(p.change_note, s.change_note, 'third join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NOT NULL THEN CONCAT(s.change_note, 'third join; ')
           WHEN s.change_note IS NULL
                AND p.change_note IS NOT NULL THEN CONCAT(p.change_note, 'third join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NULL THEN 'third join; '
       END AS change_note 
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hapar_id,
                               year,
                               land_use_area); --3,534 rows

--delete from original table where join above
WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_permanent AS t USING joined_ids AS a  
WHERE t.claim_id_p = a.claim_id_p; -- 3,532 rows

WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_seasonal AS t USING joined_ids AS a  
WHERE t.claim_id_s = a.claim_id_s; --3,532 rows 

--fourth join on hapar_id, year 
INSERT INTO joined 
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       p.land_parcel_area AS owner_land_parcel_area,
       s.land_parcel_area AS user_land_parcel_area,
       p.bps_eligible_area AS owner_bps_eligible_area,
       s.bps_eligible_area AS user_bps_eligible_area,
       p.bps_claimed_area AS owner_bps_claimed_area,
       s.bps_claimed_area AS user_bps_claimed_area,
       p.verified_exclusion AS owner_verified_exclusion,
       s.verified_exclusion AS user_verified_exclusion,
       p.land_use_area AS owner_land_use_area,
       s.land_use_area AS user_land_use_area,
       p.land_use AS owner_land_use,
       s.land_use AS user_land_use,
       p.land_activity AS owner_land_activity,
       s.land_activity AS user_land_activity,
       p.application_status AS owner_application_status,
       s.application_status AS user_application_status,
       p.land_leased_out,
       p.lfass_flag AS owner_lfass_flag,
       s.lfass_flag AS user_lfass_flag,
       CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
       year,
       CASE
           WHEN p.change_note IS NOT NULL
                AND s.change_note IS NOT NULL THEN CONCAT(p.change_note, s.change_note, 'fourth join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NOT NULL THEN CONCAT(s.change_note, 'fourth join; ')
           WHEN s.change_note IS NULL
                AND p.change_note IS NOT NULL THEN CONCAT(p.change_note, 'fourth join; ')
           WHEN p.change_note IS NULL
                AND s.change_note IS NULL THEN 'fourth join; '
       END AS change_note 
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hapar_id,
                               year); --3,011 rows

--delete from original table where join above
WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_permanent AS t USING joined_ids AS a  
WHERE t.claim_id_p = a.claim_id_p; -- 2,351 rows 

WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_seasonal AS t USING joined_ids AS a  
WHERE t.claim_id_s = a.claim_id_s; --2,523 rows 

--last mutually exclusive ones
INSERT INTO combine
SELECT mlc_hahol_id AS owner_mlc_hahol_id,
       NULL :: BIGINT AS user_mlc_hahol_id,
       habus_id AS owner_habus_id,
       NULL :: BIGINT AS user_habus_id,
       hahol_id AS owner_hahol_id,
       NULL :: BIGINT AS user_hahol_id,
       hapar_id,
       land_parcel_area AS owner_land_parcel_area,
       NULL :: BIGINT AS user_land_parcel_area,
       bps_eligible_area AS owner_bps_eligible_area,
       NULL :: BIGINT AS user_bps_eligible_area,
       bps_claimed_area AS owner_bps_claimed_area,
       NULL :: BIGINT AS user_bps_claimed_area,
       verified_exclusion AS owner_verified_exclusion,
       NULL :: BIGINT AS user_verified_exclusion,
       land_use_area AS owner_land_use_area,
       NULL :: BIGINT AS user_land_use_area,
       land_use AS owner_land_use,
       NULL :: VARCHAR AS user_land_use,
       land_activity AS owner_land_activity,
       NULL :: VARCHAR AS user_land_activity,
       application_status AS owner_application_status,
       NULL :: VARCHAR AS user_application_status,
       land_leased_out,
       lfass_flag AS owner_lfass_flag,
       NULL :: VARCHAR AS user_lfass_flag,
       claim_id_p AS claim_id,
       year,
       change_note
FROM temp_permanent;  --last 42,408 rows 

INSERT INTO combine 
SELECT NULL :: BIGINT AS owner_mlc_hahol_id,
       mlc_hahol_id AS user_mlc_hahol_id,
       NULL :: BIGINT AS owner_habus_id,
       habus_id AS user_habus_id,
       NULL :: BIGINT AS owner_hahol_id,
       hahol_id AS user_hahol_id,
       hapar_id,
       NULL :: BIGINT AS owner_land_parcel_area, 
       land_parcel_area AS user_land_parcel_area,
       NULL :: BIGINT AS owner_bps_eligible_area,
       bps_eligible_area AS user_bps_eligible_area,
       NULL :: BIGINT AS owner_bps_claimed_area,
       bps_claimed_area AS user_bps_claimed_area,
       NULL :: BIGINT AS owner_verified_exclusion,
       verified_exclusion AS user_verified_exclusion,
       NULL :: BIGINT AS owner_land_use_area,
       land_use_area AS user_land_use_area,
       NULL :: VARCHAR AS owner_land_use,
       land_use AS user_land_use,
       NULL :: VARCHAR AS owner_land_activity,
       land_activity AS user_land_activity,
       NULL :: VARCHAR AS owner_application_status,
       application_status AS user_application_status,
       land_leased_out,
       NULL :: VARCHAR AS owner_lfass_flag,
       lfass_flag AS user_lfass_flag,
       claim_id_s AS claim_id,
       year,
       change_note
FROM temp_seasonal; --last 20,318 rows

DROP TABLE temp_permanent; 
DROP TABLE temp_seasonal;

--*Step 7. Last cleaning
--find owners based on LLO flag and change them from user to owner from mutually exclusive table
UPDATE combine
SET owner_mlc_hahol_id = user_mlc_hahol_id,
    user_mlc_hahol_id = NULL,
    owner_habus_id = user_habus_id,
    user_habus_id = NULL,
    owner_hahol_id = user_hahol_id,
    user_hahol_id = NULL,
    owner_land_parcel_area = user_land_parcel_area,
    user_land_parcel_area = NULL,
    owner_bps_eligible_area = user_bps_eligible_area,
    user_bps_eligible_area = NULL,
    owner_bps_claimed_area = user_bps_claimed_area,
    user_bps_claimed_area = NULL,
    owner_verified_exclusion = user_verified_exclusion,
    user_verified_exclusion = NULL,
    owner_land_use_area = user_land_use_area,
    user_land_use_area = NULL,
    owner_land_use = user_land_use,
    user_land_use = NULL,
    owner_land_activity = user_land_activity,
    user_land_activity = NULL,
    owner_application_status = user_application_status,
    user_application_status = NULL,
    owner_lfass_flag = user_lfass_flag,
    user_lfass_flag = NULL,
    claim_id = (CASE
                    WHEN claim_id LIKE '%-01' THEN 'P' || TRIM('S'
                                                               from claim_id) || TRIM(TRAILING '-01') || '-01'
                    ELSE 'P' || TRIM('S'
                                     from claim_id) || '-01'
                END),
    change_note = (CASE
                       WHEN change_note LIKE '%record%' THEN 'S record moved from seasonal to permanent sheet based on LLO yes; '
                       ELSE CONCAT(change_note, 'S record moved from seasonal to permanent sheet based on LLO yes; ')
                   END)
WHERE land_leased_out = 'Y'
    AND user_land_use IS NOT NULL
    AND user_bps_claimed_area = 0; --updates 347 records

--*Step 8. Combine all into final table
DROP TABLE IF EXISTS last_step;
CREATE TEMP TABLE last_step AS
SELECT owner_mlc_hahol_id,
       user_mlc_hahol_id,
       owner_habus_id,
       user_habus_id,
       owner_hahol_id,
       user_hahol_id,
       hapar_id,
       CASE
           WHEN owner_land_parcel_area IS NULL THEN user_land_parcel_area
           WHEN user_land_parcel_area IS NULL THEN owner_land_parcel_area
       END AS land_parcel_area,
       CASE
           WHEN owner_bps_eligible_area IS NULL THEN user_bps_eligible_area
           WHEN user_bps_eligible_area IS NULL THEN owner_bps_eligible_area
       END AS bps_eligible_area,
       owner_bps_claimed_area,
       user_bps_claimed_area,
       CASE
           WHEN owner_verified_exclusion IS NULL THEN user_verified_exclusion
           WHEN user_verified_exclusion IS NULL THEN owner_verified_exclusion
       END AS verified_exclusion,
       owner_land_use_area,
       user_land_use_area,
       owner_land_use,
       user_land_use,
       CASE
           WHEN owner_land_activity IS NULL THEN user_land_activity
           WHEN user_land_activity IS NULL THEN owner_land_activity
       END AS land_activity,
       CASE
           WHEN owner_application_status IS NULL THEN user_application_status
           WHEN user_application_status IS NULL THEN owner_application_status
       END AS application_status,
       land_leased_out,
       owner_lfass_flag,
       user_lfass_flag,
       claim_id,
       year,
       change_note
FROM combine;

INSERT INTO last_step 
SELECT owner_mlc_hahol_id,
       user_mlc_hahol_id,
       owner_habus_id,
       user_habus_id,
       owner_hahol_id,
       user_hahol_id,
       hapar_id,
       land_parcel_area, --TODO do a check on this
       bps_eligible_area, --TODO do a check on this
       owner_bps_claimed_area,
       user_bps_claimed_area,
       verified_exclusion, --TODO do a check on this
       owner_land_use_area,
       user_land_use_area,
       owner_land_use,
       user_land_use,
       user_land_activity, --TODO do a check on this
       user_application_status, --TODO do a check on this
       land_leased_out,
       owner_lfass_flag,
       user_lfass_flag,
       claim_id,
       year,
       change_note
FROM joined--2,044,091

--infer NON-SAF renter where LLO yes 
UPDATE last_step 
SET user_land_use = 'NON_SAF',
change_note = CONCAT(change_note, 'infer non-SAF renter; ')
WHERE user_habus_id IS NULL AND land_leased_out = 'Y'; --updates 7,533 rows

--infer NON-SAF owner for mutually exclusive users
UPDATE last_step 
SET owner_land_use = 'NON_SAF',
change_note = CONCAT(change_note, 'infer non-SAF owner; ')
WHERE owner_habus_id IS NULL; --updates 114,513 rows