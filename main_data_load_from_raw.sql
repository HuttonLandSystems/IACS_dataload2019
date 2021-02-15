
--*Step 1. Create temp tables, remove select rows, add select columns
-- DROPPED COLUMNS: payment_region, organic_status
-- DROPPED DATA: all year 2019 data, ((land_use = 'EXCL' OR land_use = 'DELETED_LANDUSE') AND (land_use_area = 0 OR land_use_area IS NULL)), 
--               duplicate records from payment_region, NULL hapar_id and land_use = ' '
-- recast claim_id column to accept multiple values
-- rename claim_id_p/s so no problems with unique ids between tables
-- create a table for excluded land use codes for use later

--*Step 2. Fix land_parcel_area IS NULL or 0
-- infer land_parcel_area where same hapar_id in different year 
-- infer land_parcel_area from land_use_area in same row
-- delete where land_parcel_area IS NULL/0 AND land_use_area = 0

--*Step 3. Fix land_use_area IS NULL or 0 
-- copy land_parcel_area for single claims where land_parcel_area = bps_eligible_area
-- copy values from other years where same land_use
-- adjust bps_claimed_area to match land_parcel_area WHERE bps_claimed_area > land_parcel_area
-- adjust land_use_area to match bps_claimed_area
-- delete remaining NULL land_use_area

--*Step 4. Join
-- first join on hapar_id, year, land_use, land_use_area
--      delete from original table where perfect join above
-- second join on hapar_id, year, land_use 
-- third join on hapar_id, year, land_use_area
-- fourth join on hapar_id, year 
-- deletes problem fourth joins based on specific criteria
-- delete from original table where join above

--*STEP 5. Clean up 
-- move leftover mutually exclusive ones to diff tables 

--*Step 6. Combine ALL rows into final table
-- mark rows in joined where owner_land_parcel_area <> user_land_parcel_area
-- mark rows in joined where owner_bps_eligible_area <> user_bps_eligible_area
-- mark rows in joined where owner_verified_exclusion <> user_verified_exclusion
-- mark rows in joined where owner_land_activity <> user_land_activity 
-- mark rows in joined where owner_application_status <> user_application_status
-- move joined data into last table
-- infer NON-SAF renter where LLO yes 
-- infer NON-SAF owner for mutually exclusive users
-- delete zero excluded land use
-- make land_parcel_area match for hapar_ids and year

--*STEP 7: Mark remaining errors // in order of frequency ASC
-- alter table: add error_log to count things that are wrong 
-- error: owner_hahol_id <> user_hahol_id
-- error: multiple permanent businesses declaring on same hapar_id
-- error: sum(owner_land_parcel_area) <> sum(user_land_parcel_area)
-- error: sum(owner_bps_eligible_area) <> sum(user_bps_eligible_area)
-- error: doubled user claims in join (one to many)
-- error: seasonal renter with LLO yes
-- error: doubled owner claims in join (many to one)
-- error: multiple seasonal businesses declaring on same hapar_id
-- error: owner_land_use <> user_land_use
-- error: sum(land_use_area) > land_parcel_area
-- error: sum(bps_claimed_area) > land_parcel_area

--*STEP 8: create final table 


-- EDIT on 15 February 2021 because DWJ found that user_bps_claimed_area and user_land_use_area were Integer type fields. The fix gained 33,495 rows. 
--------------------------------------------------------------------------------------------------------------------------------------------


--*Step 1. Create temp tables, remove select rows, add select columns
-- DROPPED COLUMNS: payment_region, organic_status
-- DROPPED DATA: all year 2019 data, ((land_use = 'EXCL' OR land_use = 'DELETED_LANDUSE') AND (land_use_area = 0 OR land_use_area IS NULL)), 
--               duplicate records from payment_region, NULL hapar_id and land_use = ' '
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
WHERE row_num < 2 -- removes 55,250 rows
    AND hapar_id IS NOT NULL -- removes 609 rows
    AND land_use <> '' -- removes 1,761 rows
    AND year <> 2019 -- removes 681,711 ROWS
    AND application_status NOT LIKE '%Wait%'; --removes 35,966 rows 

DELETE
FROM temp_permanent
WHERE (land_use = 'EXCL'
        OR land_use = 'DELETED_LANDUSE')
       AND (land_use_area = 0
            OR land_use_area IS NULL); -- removes 720,741 rows

ALTER TABLE temp_permanent ADD change_note VARCHAR;
---------------------------------------------------------------------1,898,643 in temp_permanent

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
    AND hapar_id IS NOT NULL -- removes 35 rows
    AND land_use <> '' -- removes 707 rows
    AND year <> 2019 -- removes 30,403 rows
    AND application_status NOT LIKE '%Wait%'; -- removes 2,939 rows

DELETE
FROM temp_seasonal
WHERE (land_use = 'EXCL'
        OR land_use = 'DELETED_LANDUSE')
       AND (land_use_area = 0
            OR land_use_area IS NULL); -- removes 76,649 rows

ALTER TABLE temp_seasonal ADD change_note VARCHAR;
---------------------------------------------------------------------173,252 in temp_seasonal

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

-- create a table for excluded land use codes for use later
DROP TABLE IF EXISTS excl;
CREATE TEMP TABLE excl (land_use VARCHAR(30),
                                 descript VARCHAR(30));

INSERT INTO excl (land_use, descript)
VALUES ('BLU-GLS', 'Blueberries - glasshouse'), 
       ('BRA', 'Bracken'), 
       ('BUI', 'Building'), 
       ('EXCL','Generic exclusion'),
       ('FSE', 'Foreshore'), 
       ('GOR', 'Gorse'), 
       ('LLO', 'Land let out'), 
       ('MAR', 'Marsh'), 
       ('RASP-GLS', 'Raspberries - glasshouse'), 
       ('ROAD', 'Road'), 
       ('ROK', 'Rocks'), 
       ('SCB', 'Scrub'), 
       ('SCE', 'Scree'), 
       ('STRB-GLS', 'Strawberries - glasshouse'), 
       ('TOM-GLS', 'Tomatoes - glasshouse'), 
       ('TREE', 'Trees'),
       ('TREES', 'Trees'), 
       ('TURF', 'Trees'),
       ('WAT', 'Water');

--*Step 2. Fix land_parcel_area IS NULL or 0
--infer land_parcel_area where same hapar_id in different year 
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
        AND land_use_area =0); -- deletes 19 rows

DELETE
FROM temp_seasonal
WHERE (land_parcel_area IS NULL
       AND land_use_area = 0)
    OR (land_parcel_area = 0
        AND land_use_area =0); -- deletes 1 rows

--*Step 3. Fix land_use_area IS NULL or 0 
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
    AND temp_permanent.land_parcel_area = temp_permanent.bps_eligible_area; -- updates 688 rows

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
    AND temp_seasonal.land_parcel_area = temp_seasonal.bps_eligible_area; -- updates 842 rows 

-- copy values from other years where same land_use
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
    AND (p.land_use_area IS NULL
         OR p.land_use_area = 0); -- updates 73 rows

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
    AND (s.land_use_area IS NULL
         OR s.land_use_area = 0); -- updates 113 rows 

-- adjust bps_claimed_area to match land_parcel_area WHERE bps_claimed_area > land_parcel_area
UPDATE temp_permanent 
SET bps_claimed_area = land_parcel_area, 
    change_note = CONCAT(change_note, 'adjust bps_claimed_area to match land_parcel_area where bps > parcel; ')
WHERE bps_claimed_area > land_parcel_area; -- updates 660 rows 

UPDATE temp_seasonal 
SET bps_claimed_area = land_parcel_area, 
    change_note = CONCAT(change_note, 'adjust bps_claimed_area to match land_parcel_area where bps > parcel; ')
WHERE bps_claimed_area > land_parcel_area; -- updates 49 rows          

--adjust land_use_area to match bps_claimed_area
UPDATE temp_permanent
SET land_use_area = bps_claimed_area,
    change_note = CONCAT(change_note, 'adjust owner land_use_area to match bps_claimed_area; ')
WHERE bps_claimed_area <> land_use_area
    AND bps_claimed_area <> 0
    AND bps_claimed_area <= land_parcel_area; -- updates 186,183 rows

UPDATE temp_seasonal
SET land_use_area = bps_claimed_area,
    change_note = CONCAT(change_note, 'adjust user land_use_area to match bps_claimed_area; ')
WHERE bps_claimed_area <> land_use_area
    AND bps_claimed_area <> 0
    AND bps_claimed_area <= land_parcel_area;; -- updates 21,385 rows

--delete remaining NULL land_use_area
DELETE FROM 
temp_permanent 
WHERE land_use_area IS NULL; --deletes 349 rows 

DELETE FROM 
temp_seasonal
WHERE land_use_area IS NULL; --deletes 1,168 rows

--*Step 4. Join
--first join on hapar_id, year, land_use, land_use_area
DROP TABLE IF EXISTS joined;
SELECT * INTO TEMP TABLE joined
FROM
    (SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
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
            p.land_leased_out AS owner_land_leased_out,
            s.land_leased_out AS user_land_leased_out,
            p.lfass_flag AS owner_lfass_flag,
            s.lfass_flag AS user_lfass_flag,
            CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
            year,
            CASE
                WHEN p.change_note IS NOT NULL
                     AND s.change_note IS NOT NULL THEN CONCAT('first join; ', p.change_note, s.change_note)
                WHEN p.change_note IS NULL
                     AND s.change_note IS NOT NULL THEN CONCAT('first join; ', s.change_note)
                WHEN s.change_note IS NULL
                     AND p.change_note IS NOT NULL THEN CONCAT('first join; ', p.change_note)
                WHEN p.change_note IS NULL
                     AND s.change_note IS NULL THEN 'first join; '
            END AS change_note
     FROM temp_permanent AS p
     JOIN temp_seasonal AS s USING (hapar_id,
                                    year,
                                    land_use,
                                    land_use_area)) foo
WHERE owner_land_leased_out = 'Y'
    OR (owner_land_leased_out = 'N'
        AND owner_bps_claimed_area = 0); --33,481 rows

--delete from original table where perfect join above
WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_permanent AS t USING joined_ids AS a  
WHERE t.claim_id_p = a.claim_id_p; -- 33,108 rows

WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p,
       SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_seasonal AS t USING joined_ids AS a  
WHERE t.claim_id_s = a.claim_id_s; --33,481 rows                                

--second join on hapar_id, year, land_use 
WITH all_joined AS (
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
       p.land_leased_out AS owner_land_leased_out,
       s.land_leased_out AS user_land_leased_out, 
       p.lfass_flag AS owner_lfass_flag,
       s.lfass_flag AS user_lfass_flag,
       CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
       year,
       CASE
           WHEN p.change_note IS NOT NULL
                AND s.change_note IS NOT NULL THEN CONCAT('second join; ', p.change_note, s.change_note)
           WHEN p.change_note IS NULL
                AND s.change_note IS NOT NULL THEN CONCAT('second join; ', s.change_note)
           WHEN s.change_note IS NULL
                AND p.change_note IS NOT NULL THEN CONCAT('second join; ', p.change_note)
           WHEN p.change_note IS NULL
                AND s.change_note IS NULL THEN 'second join; '
       END AS change_note 
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hapar_id,
                               year,
                               land_use))
INSERT INTO joined 
SELECT * 
FROM all_joined 
WHERE claim_id NOT IN (SELECT claim_id FROM joined); -- 16,447 rows

--third join on hapar_id, year, land_use_area
WITH all_joined AS (
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
       p.land_leased_out AS owner_land_leased_out,
       s.land_leased_out AS user_land_leased_out, 
       p.lfass_flag AS owner_lfass_flag,
       s.lfass_flag AS user_lfass_flag,
       CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
       year,
       CASE
           WHEN p.change_note IS NOT NULL
                AND s.change_note IS NOT NULL THEN CONCAT('third join; ', p.change_note, s.change_note)
           WHEN p.change_note IS NULL
                AND s.change_note IS NOT NULL THEN CONCAT('third join; ', s.change_note)
           WHEN s.change_note IS NULL
                AND p.change_note IS NOT NULL THEN CONCAT('third join; ', p.change_note)
           WHEN p.change_note IS NULL
                AND s.change_note IS NULL THEN 'third join; '
       END AS change_note 
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hapar_id,
                               year,
                               land_use_area))
INSERT INTO joined 
SELECT * 
FROM all_joined 
WHERE claim_id NOT IN (SELECT claim_id FROM joined); --3,373 rows

--fourth join on hapar_id, year 
WITH all_joined AS
    (SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
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
            p.land_leased_out AS owner_land_leased_out,
            s.land_leased_out AS user_land_leased_out,
            p.lfass_flag AS owner_lfass_flag,
            s.lfass_flag AS user_lfass_flag,
            CONCAT(claim_id_p, ', ', claim_id_s) AS claim_id,
            year,
            CASE
                WHEN p.change_note IS NOT NULL
                     AND s.change_note IS NOT NULL THEN CONCAT('fourth join; ', p.change_note, s.change_note)
                WHEN p.change_note IS NULL
                     AND s.change_note IS NOT NULL THEN CONCAT('fourth join; ', s.change_note)
                WHEN s.change_note IS NULL
                     AND p.change_note IS NOT NULL THEN CONCAT('fourth join; ', p.change_note)
                WHEN p.change_note IS NULL
                     AND s.change_note IS NULL THEN 'fourth join; '
            END AS change_note
     FROM temp_permanent AS p
     JOIN temp_seasonal AS s USING (hapar_id,
                                    year))
INSERT INTO joined
SELECT *
FROM all_joined
WHERE claim_id NOT IN
        (SELECT claim_id
         FROM joined); -- 9,773 rows

-- deletes problem fourth joins based on specific criteria
DELETE
FROM joined
WHERE change_note LIKE '%fourth join%'
    AND ((owner_land_use IN
              (SELECT land_use
               FROM excl)
          AND user_bps_claimed_area <> 0) -- 1,364 rows
         OR (user_land_use IN
                 (SELECT land_use
                  FROM excl)
             AND owner_bps_claimed_area <> 0) -- 483 rows
         OR (owner_land_use NOT IN
                 (SELECT land_use
                  FROM excl)
             AND user_land_use IN
                 (SELECT land_use
                  FROM excl))); --2,152 rows //  total 3,517

--delete from original table where join above
WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 1) AS claim_id_p
FROM joined)
DELETE 
FROM temp_permanent AS t USING joined_ids AS a  
WHERE t.claim_id_p = a.claim_id_p; -- 22,463 rows 

WITH joined_ids AS (
SELECT SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
FROM joined)
DELETE 
FROM temp_seasonal AS t USING joined_ids AS a  
WHERE t.claim_id_s = a.claim_id_s; -- 23,493 rows 

--*STEP 5. Clean up 
--move leftover mutually exclusive ones to diff tables 
DROP TABLE IF EXISTS combine; 
SELECT mlc_hahol_id AS owner_mlc_hahol_id,
       NULL :: BIGINT AS user_mlc_hahol_id,
       habus_id AS owner_habus_id,
       NULL :: BIGINT AS user_habus_id,
       hahol_id AS owner_hahol_id,
       NULL :: BIGINT AS user_hahol_id,
       hapar_id,
       land_parcel_area AS owner_land_parcel_area,
       NULL :: NUMERIC AS user_land_parcel_area,
       bps_eligible_area AS owner_bps_eligible_area,
       NULL :: NUMERIC AS user_bps_eligible_area,
       bps_claimed_area AS owner_bps_claimed_area,
       NULL :: NUMERIC AS user_bps_claimed_area,
       verified_exclusion AS owner_verified_exclusion,
       NULL :: NUMERIC user_verified_exclusion,
       land_use_area AS owner_land_use_area,
       NULL :: NUMERIC AS user_land_use_area,
       land_use AS owner_land_use,
       NULL :: VARCHAR AS user_land_use,
       land_activity AS owner_land_activity,
       NULL :: VARCHAR AS user_land_activity,
       application_status AS owner_application_status,
       NULL :: VARCHAR AS user_application_status,
       land_leased_out AS owner_land_leased_out,
       NULL :: VARCHAR AS user_land_leased_out, 
       lfass_flag AS owner_lfass_flag,
       NULL :: VARCHAR AS user_lfass_flag,
       claim_id_p AS claim_id,
       year,
       change_note INTO TEMP TABLE combine
FROM temp_permanent;  --1,842,704 rows 

INSERT INTO combine 
SELECT NULL :: BIGINT AS owner_mlc_hahol_id,
       mlc_hahol_id AS user_mlc_hahol_id,
       NULL :: BIGINT AS owner_habus_id,
       habus_id AS user_habus_id,
       NULL :: BIGINT AS owner_hahol_id,
       hahol_id AS user_hahol_id,
       hapar_id,
       NULL :: NUMERIC AS owner_land_parcel_area, 
       land_parcel_area AS user_land_parcel_area,
       NULL :: NUMERIC AS owner_bps_eligible_area,
       bps_eligible_area AS user_bps_eligible_area,
       NULL :: NUMERIC AS owner_bps_claimed_area,
       bps_claimed_area AS user_bps_claimed_area,
       NULL :: NUMERIC AS owner_verified_exclusion,
       verified_exclusion AS user_verified_exclusion,
       NULL :: NUMERIC AS owner_land_use_area,
       land_use_area AS user_land_use_area,
       NULL :: VARCHAR AS owner_land_use,
       land_use AS user_land_use,
       NULL :: VARCHAR AS owner_land_activity,
       land_activity AS user_land_activity,
       NULL :: VARCHAR AS owner_application_status,
       application_status AS user_application_status,
       NULL :: VARCHAR AS owner_land_leased_out, 
       land_leased_out AS user_land_leased_out,
       NULL :: VARCHAR AS owner_lfass_flag,
       lfass_flag AS user_lfass_flag,
       claim_id_s AS claim_id,
       year,
       change_note
FROM temp_seasonal; --last 115,109 rows

DROP TABLE temp_permanent; 
DROP TABLE temp_seasonal;

--*Step 6. Combine ALL rows into final table
DROP TABLE IF EXISTS final;
CREATE TEMP TABLE final AS
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
       owner_land_leased_out, 
       user_land_leased_out, 
       owner_lfass_flag,
       user_lfass_flag,
       claim_id,
       year,
       change_note
FROM combine; -- moves 1,957,813 rows

--mark rows in joined where owner_land_parcel_area <> user_land_parcel_area
UPDATE joined 
SET change_note = CONCAT(change_note, 'assume land_parcel_area = owner_land_parcel_area when owner > user; ')
WHERE owner_land_parcel_area > user_land_parcel_area; -- updates 167 rows

UPDATE joined 
SET change_note = CONCAT(change_note, 'assume land_parcel_area = user_land_parcel_area when user > owner; ')
WHERE owner_land_parcel_area < user_land_parcel_area; -- updates 179 rows

--mark rows in joined where owner_bps_eligible_area <> user_bps_eligible_area
UPDATE joined 
SET change_note = CONCAT(change_note, 'assume bps_eligible_area = owner_bps_eligible_area when owner > user; ')
WHERE owner_bps_eligible_area > user_bps_eligible_area; -- updates 173 rows

UPDATE joined 
SET change_note = CONCAT(change_note, 'assume bps_eligible_area = user_bps_eligible_area when user > owner; ')
WHERE owner_bps_eligible_area < user_bps_eligible_area; -- updates 241 rows

--mark rows in joined where owner_verified_exclusion <> user_verified_exclusion
UPDATE joined 
SET change_note = CONCAT(change_note, 'assume verified_exclusion = owner_verified_exclusion when owner > user; ')
WHERE owner_verified_exclusion > user_verified_exclusion; -- updates 2,208 rows

UPDATE joined 
SET change_note = CONCAT(change_note, 'assume verified_exclusion = user_verified_exclusion WHEN user > owner; ')
WHERE owner_verified_exclusion < user_verified_exclusion; -- updates 2,461 rows

--mark rows in joined where owner_land_activity <> user_land_activity 
UPDATE joined 
SET change_note = CONCAT(change_note, 'owner and user land_activity choice based on assumption user knows best; ')
WHERE owner_land_activity <> user_land_activity; -- updates 40,793 rows

--mark rows in joined where owner_application_status <> user_application_status
UPDATE joined
SET change_note = CONCAT(change_note, 'application status assumed under action/assessment if either owner or user says so; ')
WHERE (owner_application_status LIKE '%Action%'
       OR user_application_status LIKE '%Action')
    AND owner_application_status <> user_application_status; -- updates 1,257 rows

-- move joined data into last table
INSERT INTO final 
SELECT owner_mlc_hahol_id,
       user_mlc_hahol_id,
       owner_habus_id,
       user_habus_id,
       owner_hahol_id,
       user_hahol_id,
       hapar_id,
       CASE
           WHEN owner_land_parcel_area > user_land_parcel_area THEN owner_land_parcel_area
           WHEN user_land_parcel_area > owner_land_parcel_area THEN user_land_parcel_area
           ELSE owner_land_parcel_area
       END AS land_parcel_area, --changes 346 rows with largest change = 5.77 ha
       -- total change = 79.63
       CASE 
            WHEN owner_bps_eligible_area > user_bps_eligible_area THEN owner_bps_eligible_area 
            WHEN user_bps_eligible_area > owner_bps_eligible_area THEN user_bps_eligible_area
            ELSE owner_bps_eligible_area
        END AS bps_eligible_area, --changes 414 rows with largest change = 273.3 ha
        -- total change = 797.27
       owner_bps_claimed_area,
       user_bps_claimed_area,
       CASE 
            WHEN owner_verified_exclusion > user_verified_exclusion THEN owner_verified_exclusion
            WHEN user_verified_exclusion > owner_verified_exclusion THEN user_verified_exclusion
            ELSE owner_verified_exclusion
        END AS verified_exclusion, --changes 4,670 rows with largest change = 4,208.76 ha
        -- total change = 50,680.66
       owner_land_use_area,
       user_land_use_area,
       owner_land_use,
       user_land_use,
       CASE 
            WHEN owner_land_activity = '' THEN user_land_activity
            WHEN user_land_activity = '' THEN owner_land_activity 
            WHEN (user_land_activity = 'No Activity' OR user_land_activity = 'Unspecified') AND owner_land_activity <> '' THEN owner_land_activity
            ELSE user_land_activity
        END AS land_activity, --changes 40,793 rows 
       CASE 
            WHEN owner_application_status = user_application_status THEN owner_application_status
            WHEN owner_application_status LIKE '%Action%' AND owner_application_status <> user_application_status THEN owner_application_status
            WHEN user_application_status LIKE '%Action%' AND owner_application_status <> user_application_status THEN user_application_status 
            ELSE owner_application_status
        END AS application_status, --changes 1,789 rows
       owner_land_leased_out,
       user_land_leased_out, 
       owner_lfass_flag,
       user_lfass_flag,
       claim_id,
       year,
       change_note
FROM joined; -- moves 59,557 rows 

-- infer NON-SAF renter where LLO yes 
UPDATE final
SET user_land_use = 'NON_SAF',
    change_note = CONCAT(change_note, 'infer non-SAF renter; ')
WHERE user_habus_id IS NULL
    AND owner_land_leased_out = 'Y'
    and owner_land_use NOT IN
        (SELECT land_use
         FROM excl); --updates 6,683 rows

-- infer NON-SAF owner for mutually exclusive users
UPDATE final
SET owner_land_use = 'NON_SAF',
    change_note = CONCAT(change_note, 'infer non-SAF owner; ')
WHERE owner_habus_id IS NULL
    AND user_land_use NOT IN
        (SELECT land_use
         FROM excl); --updates 86,648 rows

-- delete zero excluded land use
DELETE
FROM FINAL
WHERE user_land_use_area = 0
    AND user_land_use IN
        (SELECT land_use
         FROM excl); -- 42 rows

DELETE
FROM final
WHERE owner_land_use_area = 0
    AND owner_land_use IN
        (SELECT land_use
         FROM excl); -- 262 rows

-- make land_parcel_area match for hapar_ids and year (biggest diff is 2.35 ha but 98.6% are less than 0.5)
UPDATE final AS f
SET land_parcel_area = max_parcel,
    change_note = CONCAT(f.change_note, 'adjust land_parcel_area to max(parcel) where different in same year; ')
FROM
    (SELECT hapar_id,
            YEAR,
            max(land_parcel_area) AS max_parcel,
            min(land_parcel_area) AS min_parcel
     FROM FINAL
     GROUP BY hapar_id,
              YEAR) foo
JOIN FINAL USING (hapar_id,
                  year)
WHERE f.hapar_id = final.hapar_id
    AND f.year = final.year
    AND max_parcel <> min_parcel
    AND final.land_parcel_area <> max_parcel; -- updates 163 rows

--*STEP 7: Mark remaining errors // in order of frequency ASC
-- alter table: add error_log to count things that are wrong 
ALTER TABLE final ADD error_log VARCHAR;

--owner_hahol_id <> user_hahol_id
UPDATE FINAL AS f
SET error_log = CONCAT(f.error_log, 'owner_hahol_id <> user_hahol_id; ')
WHERE owner_hahol_id <> user_hahol_id; -- 54 rows 

--multiple permanent businesses declaring on same hapar_id
UPDATE final AS f
SET error_log = CONCAT(f.error_log, 'multiple permanent businesses declaring on same hapar_id; ')
FROM
    (SELECT hapar_id,
            YEAR,
            COUNT(DISTINCT owner_habus_id)
     FROM FINAL
     WHERE owner_habus_id IS NOT NULL
     GROUP BY hapar_id,
              YEAR
     ORDER BY count DESC) AS mult_seas
WHERE f.hapar_id = mult_seas.hapar_id
    AND f.year = mult_seas.year
    AND count > 1; -- 185 rows

-- sum(owner_land_parcel_area) <> sum(user_land_parcel_area)
UPDATE final AS f
SET error_log = CONCAT(f.error_log, 'sum(owner_land_parcel_area) <> sum(user_land_parcel_area); ')
FROM
    (SELECT hapar_id,
            year,
            sum_owner,
            sum_user
     FROM
         (SELECT hapar_id,
                 year,
                 sum(owner_land_parcel_area) AS sum_owner,
                 sum(user_land_parcel_area) AS sum_user
          FROM joined
          GROUP BY hapar_id,
                   year) foo
     WHERE sum_owner <> sum_user ) bar
JOIN final USING (hapar_id,
                  year)
WHERE f.hapar_id = final.hapar_id
    AND f.year = final.year
    AND sum_owner <> sum_user;-- 398 rows

-- sum(owner_bps_eligible_area) <> sum(user_bps_eligible_area)
UPDATE final AS f
SET error_log = CONCAT(f.error_log, 'sum(owner_bps_eligible_area) <> sum(user_bps_eligible_area); ')
FROM
    (SELECT hapar_id,
            year,
            sum_owner,
            sum_user
     FROM
         (SELECT hapar_id,
                 year,
                 sum(owner_bps_eligible_area) AS sum_owner,
                 sum(user_bps_eligible_area) AS sum_user
          FROM joined
          GROUP BY hapar_id,
                   year) foo
     WHERE sum_owner <> sum_user ) bar
JOIN final USING (hapar_id,
                  year)
WHERE f.hapar_id = final.hapar_id
    AND f.year = final.year
    AND sum_owner <> sum_user;-- 485 rows    

-- doubled user claims in join (one to many)
UPDATE final AS f 
SET error_log = CONCAT(error_log, 'doubled user claims in join (one to many); ')
FROM (
SELECT *
FROM
    (SELECT hapar_id,
            YEAR,
            claim_id_s,
            COUNT(*)
     FROM
         (SELECT hapar_id,
                 year,
                 SPLIT_PART(claim_id, ', ', 2) AS claim_id_s
          FROM final
          WHERE change_note LIKE '%join%') foo
     GROUP BY hapar_id,
              YEAR,
              claim_id_s) bar
WHERE count > 2) foobar 
WHERE SPLIT_PART(f.claim_id, ', ', 2) = claim_id_s; -- 1,185 rows       

--seasonal renter with LLO yes
UPDATE final AS f
SET error_log = CONCAT(error_log, 'seasonal renter with LLO yes; ')
WHERE user_land_leased_out = 'Y';-- 1,865 rows

-- doubled owner claims in join (many to one)
UPDATE final AS f 
SET error_log = CONCAT(error_log, 'doubled owner claims in join (many to one); ')
FROM (
SELECT *
FROM
    (SELECT hapar_id,
            YEAR,
            claim_id_p,
            COUNT(*)
     FROM
         (SELECT hapar_id,
                 year,
                 SPLIT_PART(claim_id, ', ', 1) AS claim_id_p
          FROM final
          WHERE change_note LIKE '%join%') foo
     GROUP BY hapar_id,
              YEAR,
              claim_id_p) bar
WHERE count > 1) foobar 
WHERE SPLIT_PART(f.claim_id, ', ', 1) = claim_id_p; -- 7,410 rows

--multiple seasonal businesses declaring on same hapar_id
UPDATE final AS f
SET error_log = CONCAT(f.error_log, 'multiple seasonal businesses declaring on same hapar_id; ')
FROM
    (SELECT hapar_id,
            YEAR,
            COUNT(DISTINCT user_habus_id)
     FROM FINAL
     WHERE user_habus_id IS NOT NULL
     GROUP BY hapar_id,
              YEAR
     ORDER BY count DESC) AS mult_seas
WHERE f.hapar_id = mult_seas.hapar_id
    AND f.year = mult_seas.year
    AND count > 1; -- 6,016 rows

--owner_land_use <> user_land_use
UPDATE final AS f
SET error_log = CONCAT(f.error_log, 'owner_land_use <> user_land_use; ')
WHERE owner_land_use <> user_land_use
    AND owner_land_use NOT IN
        (SELECT land_use
         FROM excl)
    AND user_land_use NOT IN
        (SELECT land_use
         FROM excl) 
    AND owner_land_use <> 'NON_SAF' 
    AND user_land_use <> 'NON_SAF'; -- 8,376 rows

--sum(land_use_area) > land_parcel_area
UPDATE final AS f
SET error_log = CONCAT(f.error_log, 'sum(land_use_area) > land_parcel_area; ')
FROM
    ( SELECT hapar_id,
             YEAR,
             sum,
             land_parcel_area
     FROM
         (SELECT hapar_id,
                 YEAR,
                 sum(all_bps)
          FROM
              (SELECT hapar_id,
                      YEAR,
                      CASE
                          WHEN owner_land_use_area IS NULL THEN user_land_use_area
                          WHEN user_land_use_area IS NULL THEN owner_land_use_area
                          ELSE owner_land_use_area
                      END AS all_bps
               FROM FINAL) foo
          GROUP BY hapar_id,
                   YEAR) bar
JOIN FINAL USING (hapar_id,
                  year)) final
WHERE f.hapar_id = final.hapar_id
    AND f.year = final.year 
    AND sum > final.land_parcel_area; -- 19,745 rows         

--sum(bps_claimed_area) > land_parcel_area
UPDATE final AS f
SET error_log = CONCAT(f.error_log, 'sum(bps_claimed_area) > land_parcel_area; ')
FROM
    ( SELECT hapar_id,
             YEAR,
             sum,
             land_parcel_area
     FROM
         (SELECT hapar_id,
                 YEAR,
                 sum(all_bps)
          FROM
              (SELECT hapar_id,
                      YEAR,
                      CASE
                          WHEN owner_bps_claimed_area IS NULL THEN user_bps_claimed_area
                          WHEN user_bps_claimed_area IS NULL THEN owner_bps_claimed_area
                          ELSE owner_bps_claimed_area + user_bps_claimed_area 
                      END AS all_bps
               FROM FINAL) foo
          GROUP BY hapar_id,
                   YEAR) bar
JOIN FINAL USING (hapar_id,
                  year)) final
WHERE f.hapar_id = final.hapar_id
    AND f.year = final.year 
    AND sum > final.land_parcel_area; -- 4,330 rows

--*STEP 8: create final table 
DROP TABLE IF EXISTS ladss.saf_iacs_2016_2017_2018; 
CREATE TABLE ladss.saf_iacs_2016_2017_2018 AS 
SELECT * 
FROM final;
-- final count 2,017,066

--Permissions
GRANT ALL ON ladss.saf_iacs_2016_2017_2018 TO dw40462;
GRANT ALL ON ladss.saf_iacs_2016_2017_2018 TO dm40247;




