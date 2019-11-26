/*
After preliminary cleaning

1,926,682 in perm table     1,826,482 perm only		100,200 to join
175,489 in seas table       96,936 seas only		78,553 to join
2,102,171 total

--*2019 Data Load
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
--!     sum land_use_area where all else is match and combine claim_ids = not sure I should be doing this
            delete single areas where summed above
        copy land_parcel_area for single claims where land_parcel_area = bps_eligible_area
        update NULL land_use_areas with inferred values from other years
        removes records "Waiting for deadline/inspection" AND (bps_eligible_area = 0 and percent = 0)
--!     impute records to fix missing land_use_area values --removed
--!     mark imputed records that match existing record with NULL/0 land_use_area
--!         delete matched imputed records
        infer land_use_area from bps_claimed_area where 0 
        delete land_use_area IS NULL 

4. Find renter records in wrong tables ( sum(land_use_area) > land_parcel_area )
        finds multiple businesses claiming on same land in permanent table and marks them as seasonal, and vice versa
        finds multiple businesses claiming on same land in permanent table and marks land_use = EXCL as seasonal without changing LLO flag, and vice versa
        mark owners in seasonal table by LLO flag 
        marks other associated rows to move to permanent based on above
        moves marked records to respective tables
            
5. Move rows into separate tables       
        move mutually exclusive hapar_ids to separate table
        separate claims on parcels (which exists in both p and s sheets but) which are only claimed by one party for one year

6. Joins       

*/

--*Step 1. Create temp tables, remove select rows, add select columns
-- DROPPED COLUMNS: payment_region, organic_status
-- DROPPED DATA: all year 2019 data, ((land_use = 'EXCL' OR land_use = 'DELETED_LANDUSE') AND (land_use_area = 0 OR land_use_area IS NULL)), 
--               duplicate records from payment_region, NULL hapar_id and land_use = ' '

--1,926,682 in all perm temp
DROP TABLE IF EXISTS temp_permanent CASCADE;
CREATE TEMP TABLE temp_permanent AS WITH subq AS
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

--sum land_use_area where all else match and combine claim_ids (except on those marked as LLO)
INSERT INTO temp_permanent
SELECT *
FROM
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            land_parcel_area,
            bps_eligible_area,
            bps_claimed_area,
            verified_exclusion,
            sum(land_use_area) AS land_use_area,
            land_use,
            land_activity,
            application_status,
            land_leased_out,
            lfass_flag,
            is_perm_flag,
            string_agg(claim_id_p :: VARCHAR, ', ') AS claim_id_p,
            year,
            CONCAT(change_note, 'summed land_use_areas where all else match / combined claim_ids; ')
     FROM temp_permanent
     GROUP BY mlc_hahol_id,
              habus_id,
              hahol_id,
              hapar_id,
              land_parcel_area,
              bps_eligible_area,
              bps_claimed_area,
              verified_exclusion,
              land_use,
              land_activity,
              application_status,
              land_leased_out,
              lfass_flag,
              is_perm_flag,
              change_note,
              year) foo
WHERE claim_id_p LIKE '%,%'
    AND land_leased_out <> 'Y'; --inserts 282 rows 

INSERT INTO temp_seasonal
SELECT *
FROM
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            land_parcel_area,
            bps_eligible_area,
            bps_claimed_area,
            verified_exclusion,
            sum(land_use_area) AS land_use_area,
            land_use,
            land_activity,
            application_status,
            land_leased_out,
            lfass_flag,
            is_perm_flag,
            string_agg(claim_id_s :: VARCHAR, ', ') AS claim_id_s,
            year,
            CONCAT(change_note, 'summed land_use_areas where all else match / combined claim_ids; ')
     FROM temp_seasonal
     GROUP BY mlc_hahol_id,
              habus_id,
              hahol_id,
              hapar_id,
              land_parcel_area,
              bps_eligible_area,
              bps_claimed_area,
              verified_exclusion,
              land_use,
              land_activity,
              application_status,
              land_leased_out,
              lfass_flag,
              is_perm_flag,
              change_note,
              year) foo
WHERE claim_id_s LIKE '%,%'
    AND land_leased_out <> 'Y'; --inserts 8 rows 

-- delete single areas where summed above
WITH sub AS
    (SELECT *
     FROM temp_permanent
     WHERE change_note LIKE '%summed%')
DELETE
FROM temp_permanent AS p
WHERE EXISTS
        (SELECT *
         FROM sub
         WHERE p.mlc_hahol_id = sub.mlc_hahol_id
             AND p.habus_id = sub.habus_id
             AND p.hahol_id = sub.hahol_id
             AND p.hapar_id = sub.hapar_id
             AND p.land_parcel_area = sub.land_parcel_area
             AND p.bps_eligible_area = sub.bps_eligible_area
             AND p.bps_claimed_area = sub.bps_claimed_area
             AND p.verified_exclusion = sub.verified_exclusion
             AND p.land_use = sub.land_use
             AND p.land_activity = sub.land_activity
             AND p.application_status = sub.application_status
             AND p.land_leased_out = sub.land_leased_out
             AND p.lfass_flag = sub.lfass_flag
             AND p.is_perm_flag = sub.is_perm_flag
             AND p.year = sub.year)
    AND p.change_note IS NULL; --removes 565 rows

WITH sub AS
    (SELECT *
     FROM temp_seasonal
     WHERE change_note LIKE '%summed%')
DELETE
FROM temp_seasonal AS p
WHERE EXISTS
        (SELECT *
         FROM sub
         WHERE p.mlc_hahol_id = sub.mlc_hahol_id
             AND p.habus_id = sub.habus_id
             AND p.hahol_id = sub.hahol_id
             AND p.hapar_id = sub.hapar_id
             AND p.land_parcel_area = sub.land_parcel_area
             AND p.bps_eligible_area = sub.bps_eligible_area
             AND p.bps_claimed_area = sub.bps_claimed_area
             AND p.verified_exclusion = sub.verified_exclusion
             AND p.land_use = sub.land_use
             AND p.land_activity = sub.land_activity
             AND p.application_status = sub.application_status
             AND p.land_leased_out = sub.land_leased_out
             AND p.lfass_flag = sub.lfass_flag
             AND p.is_perm_flag = sub.is_perm_flag
             AND p.year = sub.year)
    AND p.change_note IS NULL; --removes 16 rows

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

-- Removes records "Waiting for deadline/inspection" AND (bps_eligible_area = 0 and percent = 0)
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

--TODO          THis is where I had impute

--infer land_use_area from bps_claimed_area where 0 
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
    AND bps_claimed_area <> 0; --update 1 rows

--delete land_use_area IS NULL 
DELETE FROM temp_permanent 
WHERE land_use_area IS NULL; --removes 353 rows

DELETE FROM temp_seasonal 
WHERE land_use_area IS NULL; --removes 1187 rows

--*STEP 4. Find renter records in wrong tables 
--finds multiple businesses claiming on same land in permanent table and marks them as seasonal, and vice versa
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
                                  FROM t.claim_id_p) || '_01',
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
    AND t.mlc_hahol_id = a.mlc_hahol_id
    AND t.habus_id = a.habus_id
    AND t.hahol_id = a.hahol_id
    AND t.hapar_id = a.hapar_id
    AND t.year = a.year; --updates 26 rows

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
                                  from t.claim_id_s) || '_01',
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
    AND t.year = a.year; --updates 1,178 rows

--mark owners in seasonal table by LLO flag 
UPDATE temp_seasonal
SET is_perm_flag = 'Y',
    claim_id_s = 'P' || TRIM('S'
                             from claim_id_s) || '_01',
    change_note = CONCAT(change_note, 'P record moved from seasonal to permanent sheet; ')
WHERE land_leased_out = 'Y'
    AND claim_id_s NOT LIKE '%P%'; --updates 1,432 rows

--moves marked records to respective tables
INSERT INTO temp_permanent 
SELECT * 
FROM temp_seasonal 
WHERE claim_id_s LIKE '%P%';
DELETE FROM temp_seasonal 
WHERE claim_id_s LIKE '%P%'; --moves 2,610 rows

INSERT INTO temp_seasonal 
SELECT * 
FROM temp_permanent 
WHERE claim_id_p LIKE '%S%';
DELETE FROM temp_permanent 
WHERE claim_id_p LIKE '%S%'; -- moves 23 rows 

---------------------------------------TODO GOOD up to this point ------------------------------------------- 2,100,285 total

--*STEP 6. Combine
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
         FROM temp_seasonal); --moves 1,828,877 rows
DELETE
FROM temp_permanent AS t USING combine
WHERE t.hapar_id = combine.hapar_id;

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
         FROM temp_permanent); --move 94,037 rows
DELETE
FROM temp_seasonal AS t USING combine
WHERE t.hapar_id = combine.hapar_id;

--separate claims on parcels (which exists in both p and s sheets but) which are only claimed by one party for one year
--      [join where hapar_id match but year doesn't, and NOT IN join using (hapar_id, year)]
DROP TABLE WHERE EXISTS p_only;
WITH sub AS
    (SELECT CONCAT (hapar_id, ', ', YEAR)
     FROM temp_permanent p
     JOIN temp_seasonal s USING (hapar_id,
                                 year)
     GROUP BY hapar_id,
              year),
     p_sub AS
    (SELECT SPLIT_PART(p_id, ', ', 1):: int AS hapar_id,
            SPLIT_PART(p_id, ', ', 2):: int AS YEAR
     FROM
         (SELECT DISTINCT p_id
          FROM
              (SELECT CONCAT(hapar_id, ', ', year) AS p_id
               FROM temp_permanent) foo
          WHERE p_id NOT IN
                  (SELECT *
                   FROM sub)) foo)
SELECT * INTO TEMP TABLE p_only
FROM p_sub
JOIN temp_permanent USING (hapar_id,
                           year);
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
FROM p_only;
DELETE
FROM temp_permanent AS t USING combine
WHERE t.claim_id_p = combine.claim_id; -- moves 39,671 rows to combine table

WITH sub AS
    (SELECT CONCAT (hapar_id, ', ', YEAR)
     FROM temp_permanent p
     JOIN temp_seasonal s USING (hapar_id,
                                 year)
     GROUP BY hapar_id,
              year),
     s_sub AS
    (SELECT SPLIT_PART(s_id, ', ', 1):: int AS hapar_id,
            SPLIT_PART(s_id, ', ', 2):: int AS YEAR
     FROM
         (SELECT DISTINCT s_id
          FROM
              (SELECT CONCAT(hapar_id, ', ', year) AS s_id
               FROM temp_seasonal) foo
          WHERE s_id NOT IN
                  (SELECT *
                   FROM sub)) foo)
SELECT * INTO TEMP TABLE s_only
FROM s_sub
JOIN temp_seasonal USING (hapar_id,
                          year); 
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
FROM s_only;
DELETE
FROM temp_seasonal AS t USING combine
WHERE t.claim_id_s = combine.claim_id; --moves 16,575 rows to combine table

--! up to this point 
61,037 perm
61,088 seasonal    

