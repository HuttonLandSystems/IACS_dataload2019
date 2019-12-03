--moves single claims occuring only once in all tables to separate table
DROP TABLE IF EXISTS singles_p CASCADE;
WITH sub AS
    (SELECT *
     FROM
         (SELECT hapar_id,
                 year,
                 string_agg,
                 LENGTH(string_agg)
          FROM
              (SELECT hapar_id,
                      year,
                      string_agg(is_perm_flag :: VARCHAR, '')
               FROM temp_permanent
               WHERE hapar_id NOT IN
                       (SELECT DISTINCT hapar_id
                        FROM temp_seasonal)
               GROUP BY hapar_id,
                        YEAR) foo) foo2
     WHERE length = 1)
SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       land_parcel_area,
       bps_eligible_area,
       bps_claimed_area,
       verified_exclusion,
       land_use_area,
       land_use,
       land_activity,
       application_status,
       land_leased_out,
       lfass_flag,
       is_perm_flag,
       claim_id_p,
       year,
       change_note INTO TEMP TABLE singles_p
FROM
    (SELECT *
     FROM temp_permanent AS t
     JOIN sub USING (hapar_id,
                     year)) foo; -- moves 746,623 rows to singles_p

DELETE FROM temp_permanent AS t USING singles_p 
WHERE t.hapar_id = singles_p.hapar_id AND t.year = singles_p.year; -- deletes 746,623 rows from big table

DROP TABLE IF EXISTS singles_s CASCADE;
WITH sub AS
    (SELECT *
     FROM
         (SELECT hapar_id,
                 year,
                 string_agg,
                 LENGTH(string_agg)
          FROM
              (SELECT hapar_id,
                      year,
                      string_agg(is_perm_flag :: VARCHAR, '')
               FROM temp_seasonal
               WHERE hapar_id NOT IN
                       (SELECT DISTINCT hapar_id
                        FROM temp_permanent)
               GROUP BY hapar_id,
                        YEAR) foo) foo2
     WHERE length = 1)
SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       land_parcel_area,
       bps_eligible_area,
       bps_claimed_area,
       verified_exclusion,
       land_use_area,
       land_use,
       land_activity,
       application_status,
       land_leased_out,
       lfass_flag,
       claim_id_s,
       year,
       change_note INTO TEMP TABLE singles_s
FROM
    (SELECT *
     FROM temp_seasonal AS t
     JOIN sub USING (hapar_id,
                     year)) foo; -- moves 46,035 rows to singles_s

DELETE FROM temp_seasonal AS t USING singles_s 
WHERE t.hapar_id = singles_s.hapar_id AND t.year = singles_s.year; -- deletes 46,035 rows from big table



--remove duplicate entries with blank or no land_activity and lfass_flag = 'N' - Fixes doubled sum(land_use_area)
WITH lua AS
    (SELECT hapar_id,
            land_parcel_area,
            year,
            SUM(land_use_area) AS sum_lua
     FROM temp_permanent
     GROUP BY hapar_id,
              land_parcel_area,
              year)
DELETE FROM temp_permanent
WHERE claim_id_p IN (
SELECT claim_id_p
FROM lua
JOIN temp_permanent USING (hapar_id,
                 land_parcel_area,
                 year)
WHERE ((sum_lua/land_parcel_area)*100) = 200
    AND (land_activity = '' OR land_activity LIKE '%No Activity%')
    AND lfass_flag = 'N'); --removes 63 rows

WITH lua AS
    (SELECT hapar_id,
            land_parcel_area,
            year,
            SUM(land_use_area) AS sum_lua
     FROM temp_seasonal
     GROUP BY hapar_id,
              land_parcel_area,
              year)
DELETE FROM temp_seasonal
WHERE claim_id_s IN (
SELECT claim_id_s
FROM lua
JOIN temp_seasonal USING (hapar_id,
                 land_parcel_area,
                 year)
WHERE ((sum_lua/land_parcel_area)*100) = 200
    AND (land_activity = '' OR land_activity LIKE '%No Activity%')
    AND lfass_flag = 'N'); --removes 1,117‬ rows

--infers land_use_area = 0 where NULL
UPDATE temp_permanent 
SET land_use_area = 0,
change_note = CONCAT (change_note, 'land_use_area inferred 0 where NULL; ')
WHERE land_use_area IS NULL; -- updates 168 rows

UPDATE temp_seasonal 
SET land_use_area = 0,
change_note = CONCAT (change_note, 'land_use_area inferred 0 where NULL; ')
WHERE land_use_area IS NULL; -- updates 916 rows

--marks renters in permanent table for removal to seasonal table -- only good for doubled lua
UPDATE temp_permanent AS t
SET claim_id_p = CONCAT('s', t.claim_id_p),
    change_note = CONCAT(t.change_note, 'change to seasonal record; ')
FROM temp_permanent
JOIN
    (SELECT hapar_id,
            year
     FROM
         (SELECT t.mlc_hahol_id,
                 t.habus_id,
                 t.hahol_id,
                 t.hapar_id,
                 t.land_activity,
                 t.YEAR,
                 string_agg(t.land_leased_out, ''),
                 ROW_NUMBER () OVER (PARTITION BY hapar_id,
                                                  YEAR
                                     ORDER BY hapar_id,
                                              year)
          FROM temp_permanent AS t
          JOIN
              (SELECT *
               FROM temp_permanent
               WHERE claim_id_p IN
                       (SELECT claim_id_p
                        FROM
                            (SELECT hapar_id,
                                    land_parcel_area,
                                    year,
                                    SUM(land_use_area) AS sum_lua
                             FROM temp_permanent
                             GROUP BY hapar_id,
                                      land_parcel_area,
                                      year) lua
                        JOIN temp_permanent USING (hapar_id,
                                                   land_parcel_area,
                                                   year)
                        WHERE ((sum_lua/land_parcel_area)*100) = 200)) foo USING (hapar_id,
                                                                                  year)
          GROUP BY t.mlc_hahol_id,
                   t.habus_id,
                   t.hahol_id,
                   t.hapar_id,
                   t.land_activity,
                   t.YEAR) t
     WHERE ROW_NUMBER > 1) sub USING (hapar_id,
                                      year)
WHERE t.hapar_id = sub.hapar_id
    AND t.year = sub.year
    AND t.bps_claimed_area = t.land_use_area
    AND t.bps_claimed_area <> 0; --updates 40 rows

--marks renters in the seasonal table which have owners in same table -- only good for doubled lua
UPDATE temp_seasonal AS t
SET claim_id_s = CONCAT('s', t.claim_id_s),
    change_note = CONCAT(t.change_note, 'change to seasonal; ')
FROM temp_seasonal
JOIN
    (SELECT hapar_id,
            year
     FROM
         (SELECT t.mlc_hahol_id,
                 t.habus_id,
                 t.hahol_id,
                 t.hapar_id,
                 t.land_activity,
                 t.YEAR,
                 string_agg(t.land_leased_out, ''),
                 ROW_NUMBER () OVER (PARTITION BY hapar_id,
                                                  YEAR
                                     ORDER BY hapar_id,
                                              year)
          FROM temp_seasonal AS t
          JOIN
              (SELECT *
               FROM temp_seasonal
               WHERE claim_id_s IN
                       (SELECT claim_id_s
                        FROM
                            (SELECT hapar_id,
                                    land_parcel_area,
                                    year,
                                    SUM(land_use_area) AS sum_lua
                             FROM temp_seasonal
                             GROUP BY hapar_id,
                                      land_parcel_area,
                                      year) lua
                        JOIN temp_seasonal USING (hapar_id,
                                                   land_parcel_area,
                                                   year)
                        WHERE ((sum_lua/land_parcel_area)*100) = 200)) foo USING (hapar_id,
                                                                                  year)
          GROUP BY t.mlc_hahol_id,
                   t.habus_id,
                   t.hahol_id,
                   t.hapar_id,
                   t.land_activity,
                   t.YEAR) t
     WHERE ROW_NUMBER > 1) sub USING (hapar_id,
                                      year)
WHERE t.hapar_id = sub.hapar_id
    AND t.year = sub.year
    AND t.bps_claimed_area = t.land_use_area
    AND t.bps_claimed_area <> 0; --updates 1,047 rows

--TODO      IMPUTE starts 
--impute records to fix missing land_use_area values
WITH lua AS
    (SELECT hapar_id,
            land_parcel_area,
            year,
            sum_lua,
            percent
     FROM
         (SELECT hapar_id,
                 land_parcel_area,
                 land_use_area,
                 land_use,
                 year,
                 SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) AS sum_lua,
                 (SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) / land_parcel_area) * 100 AS percent
          FROM temp_permanent) foo
     WHERE sum_lua < land_parcel_area
         AND sum_lua > 0
     GROUP BY hapar_id,
              land_parcel_area,
              year,
              sum_lua,
              percent),
     perm AS
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            land_parcel_area,
            year,
            change_note
     FROM temp_permanent
     GROUP BY mlc_hahol_id,
              habus_id,
              hahol_id,
              hapar_id,
              land_parcel_area,
              year,
              change_note)
INSERT INTO temp_permanent (mlc_hahol_id, habus_id, hahol_id, hapar_id, land_parcel_area, bps_claimed_area, land_use_area, land_use, is_perm_flag, year, change_note)
SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       land_parcel_area,
       0 AS bps_claimed_area,
       (land_parcel_area - sum_lua) AS diff,
       'IMPUTED_RECORD',
       'Y',
       year,
       CONCAT(change_note, 'imputed record to correct difference when sum(land_use_area) < land_parcel_area; ')
FROM lua
JOIN perm USING (hapar_id,
                 land_parcel_area,
                 year); -- imputes 34,128 rows

--! seasonal sum_lua should not necessarily = land_parcel_area BUT these could be transferred to imputed LLOs later
WITH lua AS
    (SELECT hapar_id,
            land_parcel_area,
            year,
            sum_lua,
            percent
     FROM
         (SELECT hapar_id,
                 land_parcel_area,
                 land_use_area,
                 land_use,
                 year,
                 SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) AS sum_lua,
                 (SUM(land_use_area) OVER(PARTITION BY hapar_id, land_parcel_area, year) / land_parcel_area) * 100 AS percent
          FROM temp_seasonal) foo
     WHERE sum_lua < land_parcel_area
         AND sum_lua > 0
     GROUP BY hapar_id,
              land_parcel_area,
              year,
              sum_lua,
              percent),
     perm AS
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            land_parcel_area,
            year,
            change_note
     FROM temp_seasonal
     GROUP BY mlc_hahol_id,
              habus_id,
              hahol_id,
              hapar_id,
              land_parcel_area,
              year,
              change_note)
INSERT INTO temp_seasonal (mlc_hahol_id, habus_id, hahol_id, hapar_id, land_parcel_area, bps_claimed_area, land_use_area, land_use, is_perm_flag, year, change_note)
SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       land_parcel_area,
       0 AS bps_claimed_area,
       (land_parcel_area - sum_lua) AS diff,
       'IMPUTED_RECORD',
       'N',
       year,
       CONCAT(change_note, 'imputed record to correct difference when sum(land_use_area) < land_parcel_area; ')
FROM lua
JOIN perm USING (hapar_id,
                 land_parcel_area,
                 year); -- imputes 6,252 records

-- mark imputed records that match existing record with NULL/0 land_use_area
WITH impute AS
    (SELECT *
     FROM temp_permanent
     WHERE land_use = 'IMPUTED_RECORD'), -- 34,079 rows
 missing_lua AS
    (SELECT *
     FROM temp_permanent
     WHERE land_use_area IS NULL
         OR land_use_area = 0), -- 5,677 rows
 doubleLU AS
    (SELECT distinct hapar_id
     FROM
         (SELECT hapar_id,
                 year,
                 ROW_NUMBER () OVER (PARTITION BY mlc_hahol_id,
                                                  habus_id,
                                                  hahol_id,
                                                  hapar_id,
                                                  land_parcel_area,
                                                  year
                                     ORDER BY mlc_hahol_id,
                                              habus_id,
                                              hahol_id,
                                              hapar_id,
                                              land_parcel_area,
                                              year) row_num
          FROM missing_lua
          JOIN impute USING (mlc_hahol_id,
                                   habus_id,
                                   hahol_id,
                                   hapar_id,
                                   land_parcel_area,
                                   year)) foo
     WHERE row_num > 1),
 elim_doubleLU AS
    (SELECT hapar_id,
            year,
            missing_lua.land_use AS null_lu,
            missing_lua.land_use_area AS null_lua,
            impute.land_use_area AS fix_lua
     FROM missing_lua
     INNER JOIN impute USING (mlc_hahol_id,
                              habus_id,
                              hahol_id,
                              hapar_id,
                              land_parcel_area,
                              year)
     WHERE missing_lua.hapar_id NOT IN
             (SELECT *
              FROM doubleLU))
UPDATE temp_permanent AS p
SET change_note = CONCAT (p.change_note, 'MATCH TO EXISTING RECORD - SET FOR DELETION; ')
FROM (temp_permanent
JOIN elim_doubleLU USING (hapar_id, year)) sub
WHERE p.hapar_id = sub.hapar_id AND p.year = sub.year AND p.land_use = 'IMPUTED_RECORD'; -- updates 842 rows  

WITH impute AS
    (SELECT *
     FROM temp_seasonal
     WHERE land_use = 'IMPUTED_RECORD'), -- 6,228 rows
 missing_lua AS
    (SELECT *
     FROM temp_seasonal
     WHERE land_use_area IS NULL
         OR land_use_area = 0), -- 1,825 rows
 doubleLU AS
    (SELECT distinct hapar_id
     FROM
         (SELECT hapar_id,
                 year,
                 ROW_NUMBER () OVER (PARTITION BY mlc_hahol_id,
                                                  habus_id,
                                                  hahol_id,
                                                  hapar_id,
                                                  land_parcel_area,
                                                  year
                                     ORDER BY mlc_hahol_id,
                                              habus_id,
                                              hahol_id,
                                              hapar_id,
                                              land_parcel_area,
                                              year) row_num
          FROM missing_lua
          JOIN impute USING (mlc_hahol_id,
                             habus_id,
                             hahol_id,
                             hapar_id,
                             land_parcel_area,
                             year)) foo
     WHERE row_num > 1),
 elim_doubleLU AS
    (SELECT hapar_id,
            year,
            missing_lua.land_use AS null_lu,
            missing_lua.land_use_area AS null_lua,
            impute.land_use_area AS fix_lua
     FROM missing_lua
     INNER JOIN impute USING (mlc_hahol_id,
                              habus_id,
                              hahol_id,
                              hapar_id,
                              land_parcel_area,
                              year)
     WHERE missing_lua.hapar_id NOT IN
             (SELECT *
              FROM doubleLU))
UPDATE temp_seasonal AS p
SET change_note = CONCAT (p.change_note, 'MATCH TO EXISTING RECORD - SET FOR DELETION; ')
FROM
    (SELECT hapar_id,
            year
     FROM temp_seasonal
     JOIN elim_doubleLU USING (hapar_id,
                               year)) sub
WHERE p.hapar_id = sub.hapar_id
    AND p.year = sub.year
    AND (land_use_area IS NULL
         or land_use_area = 0); -- updates 381 rows  

--match marked imputed record and replace NULL/0 land_use_area with calculated area
WITH impute AS
    (SELECT *
     FROM temp_permanent
     WHERE change_note LIKE '%DELETION%'),
     missing_lua AS
    (SELECT *
     FROM temp_permanent
     WHERE land_use_area = 0
         OR land_use_area IS NULL),
     end_tbl AS
    (SELECT distinct claim_id_p,
                     fix_lua
     FROM
         (SELECT hapar_id,
                 YEAR,
                 impute.land_use_area AS fix_lua
          FROM impute
          JOIN missing_lua USING (hapar_id,
                                  YEAR)) foo
     JOIN temp_permanent AS p USING (hapar_id,
                                     year)
     WHERE p.hapar_id = foo.hapar_id
         AND p.year = foo.year
         AND land_use <> 'IMPUTED_RECORD'
         AND (land_use_area = 0
              OR land_use_area IS NULL))
UPDATE temp_permanent AS p
SET land_use_area = end_tbl.fix_lua,
	change_note = CONCAT(p.change_note, 'land_use_area calculated from missing total land_parcel_area for single claims; ')
FROM temp_permanent JOIN end_tbl USING (claim_id_p)
	WHERE p.claim_id_p = end_tbl.claim_id_p; -- updates 842 rows

WITH impute AS
    (SELECT *
     FROM temp_seasonal
     WHERE change_note LIKE '%DELETION%'),
     missing_lua AS
    (SELECT *
     FROM temp_seasonal
     WHERE land_use_area = 0
         OR land_use_area IS NULL),
     end_tbl AS
    (SELECT distinct claim_id_s,
                     fix_lua
     FROM
         (SELECT hapar_id,
                 YEAR,
                 impute.land_use_area AS fix_lua
          FROM impute
          JOIN missing_lua USING (hapar_id,
                                  YEAR)) foo
     JOIN temp_seasonal AS p USING (hapar_id,
                                     year)
     WHERE p.hapar_id = foo.hapar_id
         AND p.year = foo.year
         AND land_use <> 'IMPUTED_RECORD'
         AND (land_use_area = 0
              OR land_use_area IS NULL))
UPDATE temp_seasonal AS p
SET land_use_area = end_tbl.fix_lua,
	change_note = CONCAT(p.change_note, 'land_use_area calculated from missing total land_parcel_area for single claims; ')
FROM temp_seasonal JOIN end_tbl USING (claim_id_s)
	WHERE p.claim_id_s = end_tbl.claim_id_s; -- updates 381 rows

--delete matched imputed records
DELETE FROM temp_permanent 
WHERE change_note LIKE '%DELETION%'; -- removes 842 rows

DELETE FROM temp_seasonal 
WHERE change_note LIKE '%DELETION%'; -- removes 381 rows



--*STEP 6. Separate records with no hapar_ids in other table
        --move mutually exclusive hapar_ids to separate table
DROP TABLE IF EXISTS p_only;
CREATE TEMP TABLE p_only AS
SELECT *
FROM temp_permanent AS perm
WHERE hapar_id NOT IN
        (SELECT DISTINCT hapar_id
         FROM temp_seasonal); 
DELETE
FROM temp_permanent AS t USING p_only
WHERE t.hapar_id = p_only.hapar_id; --moves 1,110,723 rows

DROP TABLE IF EXISTS s_only;
CREATE TEMP TABLE s_only AS
SELECT * 
FROM temp_seasonal AS seas 
WHERE hapar_id NOT IN 
        (SELECT DISTINCT hapar_id 
        FROM temp_permanent); 
DELETE
FROM temp_seasonal AS t USING s_only
WHERE t.hapar_id = s_only.hapar_id; --moves 52,500 rows


--Step 2e. Impute records for LLO Yes in both tables - assume non-saf owners/renters
--! NO need to do this because it will go into end table as owner_mlc.... user_mlc...
--! can just arrange to make note in change_note about non-saf renter 
With LLO_yes AS
    (SELECT *
     FROM temp_permanent_only
     WHERE land_leased_out = 'Y')
INSERT INTO temp_seasonal_only (hapar_id, land_parcel_area, bps_eligible_area, bps_claimed_area, verified_exclusion, land_use_area, land_use, land_activity, application_status, land_leased_out, lfass_flag, is_perm_flag, year, change_note)
SELECT hapar_id,
       p.land_parcel_area,
       p.bps_eligible_area,
       p.bps_claimed_area,
       p.verified_exclusion,
       p.land_use_area,
       'IMPUTED_RECORD',
       p.land_activity,
       p.application_status,
       'N',
       p.lfass_flag,
       'N',
       year,
       CONCAT(p.change_note, 'imputed record to create non-SAF renter claim where LLO yes in permanent table; ')
FROM LLO_yes
JOIN temp_permanent_only AS p USING (hapar_id,
                                     year,
                                     land_use,
                                     land_use_area)
WHERE p.land_leased_out = 'Y'; -- imputes 5,930 rows
-- 238 LLO yes in seasonal only table (1,542 total)




--! I can't fix these because some of them are separated because its LLO and I suspect a lot of them are not marked for LLO 
--TODO      check how many duplicate land_use per year  ----------- Doug agrees that this does not necessarily need fixing
SELECT DISTINCT hapar_id
FROM
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            land_parcel_area,
            bps_eligible_area,
            bps_claimed_area,
            verified_exclusion,
            land_use_area,
            land_use,
            land_activity,
            application_status,
            land_leased_out,
            lfass_flag,
            is_perm_flag,
            claim_id_p,
            YEAR,
            ROW_NUMBER () OVER (PARTITION BY mlc_hahol_id,
                                            habus_id,
                                            hahol_id,
                                            hapar_id,
                                            land_parcel_area,
                                            land_use,
                                            year
                               ORDER BY mlc_hahol_id,
                                        habus_id,
                                        hahol_id,
                                        hapar_id,
                                        land_parcel_area,
                                        land_use,
                                        year) row_num
     FROM temp_permanent) foo
WHERE row_num > 1
--TODO -------------------------------------- 833 distinct hapar_id --- 1,189 rows

--finds multiple businesses claiming on same land in permanent table and marks land_use = EXCL as seasonal without changing LLO flag, and vice versa
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
SET claim_id_p = CONCAT('S', t.claim_id_p),
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
    AND t.year = a.year
    AND t.land_use = 'EXCL'; --updates 23 rows

--finds multiple businesses claiming on same land in seasonal table and marks land_use = EXCL as permanent without changing LLO flag, and vice versa
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
SET claim_id_s = CONCAT('P', t.claim_id_s),
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
    AND t.year = a.year
    AND t.land_use = 'EXCL'; --updates 507 rows

--marks other associated rows of same business to move to permanent based on above
WITH perms AS
    (SELECT *
     FROM
         (SELECT mlc_hahol_id,
                 habus_id,
                 hahol_id,
                 hapar_id,
                 YEAR,
                 string_agg(claim_id_s :: varchar, '') AS agg
          FROM temp_seasonal
          GROUP BY mlc_hahol_id,
                   habus_id,
                   hahol_id,
                   hapar_id,
                   YEAR) foo
     WHERE agg LIKE '%p%')
UPDATE temp_seasonal AS t
SET is_perm_flag = 'Y',
    claim_id_s = 'P' || TRIM('S' from t.claim_id_s),
    change_note = CONCAT(t.change_note, 'P record moved from seasonal to permanent sheet; ')
FROM temp_seasonal AS a
JOIN perms USING (mlc_hahol_id,
                  habus_id,
                  hahol_id,
                  hapar_id,
                  year)
WHERE t.is_perm_flag <> 'Y'
    AND t.mlc_hahol_id = a.mlc_hahol_id
    AND t.habus_id = a.habus_id
    AND t.hahol_id = a.hahol_id
    AND t.hapar_id = a.hapar_id
    AND t.year = a.year; --updates 694 rows


--Step 3. Join P and S on all possible variables
CREATE TEMP TABLE temp_combine AS
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       land_parcel_area,
       bps_eligible_area,
       bps_claimed_area,
       verified_exclusion,
       land_use_area,
       land_use,
       p.land_activity,
       p.application_status,
       p.land_leased_out AS p_LLO,
       s.land_leased_out AS s_LLO,
       p.lfass_flag,
       s.is_perm_flag,
       claim_id_p,
       claim_id_s,
       year
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hahol_id,
                               hapar_id,
                               land_parcel_area,
                               bps_eligible_area,
                               bps_claimed_area,
                               verified_exclusion,
                               land_use_area,
                               land_use,
                               year,
                               land_activity,
                               application_status,
                               lfass_flag); -- 2,233 rows (same with/out hahol_id)

--!             START JOIN              --! 
CREATE TEMP TABLE combined (owner_mlc_hahol_id int8,
                            user_mlc_hahol_id int8, 
                            owner_habus_id int8, 
                            user_habus_id int8, 
                            owner_hahol_id int8, 
                            user_hahol_id int8, 
                            hapar_id int8, 
                            land_parcel_area numeric, 
                            bps_eligible_area numeric, 
                            bps_claimed_area numeric, 
                            verified_exclusion numeric, 
                            land_use_area numeric, 
                            land_use VARCHAR, 
                            land_activity VARCHAR, 
                            application_status VARCHAR, 
                            land_leased_out VARCHAR, 
                            lfass_flag VARCHAR, 
                            is_perm_flag VARCHAR, 
                            claim_id_p int4,
                            claim_id_s int4,
                            year int2,
                            change_note VARCHAR);

INSERT INTO combined (owner_mlc_hahol_id,
                        user_mlc_hahol_id,
                        owner_habus_id,
                        user_habus_id,
                        owner_hahol_id,
                        user_hahol_id,
                        hapar_id,
                        land_parcel_area,
                        bps_eligible_area,
                        bps_claimed_area,
                        verified_exclusion,
                        land_use_area,
                        land_use,
                        land_activity,
                        application_status,
                        land_leased_out,
                        lfass_flag,
                        is_perm_flag,
                        claim_id_p,
                        claim_id_s,
                        year,
                        change_note)

WITH grp_singlehapar_p AS
    (SELECT hapar_id,
            year,
            claim_id_p
     FROM
         (SELECT hapar_id,
                 year,
                 COUNT(*)
          FROM temp_permanent
          GROUP BY hapar_id,
                   year
          ORDER BY hapar_id,
                   year) foo
     JOIN temp_permanent USING (hapar_id,
                                year)
     WHERE count = 1 ),
     grp_singlehapar_s AS
    (SELECT hapar_id,
            year,
            claim_id_s
     FROM
         (SELECT hapar_id,
                 year,
                 COUNT(*)
          FROM temp_permanent
          GROUP BY hapar_id,
                   year
          ORDER BY hapar_id,
                   year) foo
     JOIN temp_seasonal USING (hapar_id,
                               year)
     WHERE count = 1 )
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       land_parcel_area,
       bps_eligible_area,
       p.verified_exclusion, --which one to pick?
       land_use_area,
       land_use,
       p.land_activity,
       p.application_status,
       'Y' AS land_leased_out, --default to yes because join
       p.lfass_flag,
       s.is_perm_flag,
       claim_id_p,
       claim_id_s,
       year,
       p.change_note
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hahol_id,
                               hapar_id,
                               land_parcel_area,
                               bps_eligible_area,
                               land_use_area,
                               land_use,
                               year)
WHERE claim_id_p IN
        (SELECT claim_id_p
         FROM grp_singlehapar_p)
    AND claim_id_s IN
        (SELECT claim_id_s
         FROM grp_singlehapar_s)

--TODO join by hapar_id
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       p.land_parcel_area AS p_land_parcel_area,
       s.land_parcel_area AS s_land_parcel_area,
       p.bps_eligible_area AS p_bps_elig,
       s.bps_eligible_area AS s_bps_elig,
       p.bps_claimed_area AS p_bps_claim,
       s.bps_claimed_area AS s_bps_claim,
       p.verified_exclusion,
       p.land_use_area AS p_land_use_area,
       s.land_use_area AS s_land_use_area,
       land_use,
       p.land_activity,
       p.application_status,
       p.land_leased_out AS p_LLO,
       s.land_leased_out AS s_LLO,
       p.lfass_flag,
       s.is_perm_flag,
       claim_id_p,
       claim_id_s,
       year
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING ( hapar_id,
                                land_use,
                                year);



--hapar_id not in permanent  sheet, LLO yes
CREATE TEMP TABLE switch_to_perm AS 
WITH sub AS
    (SELECT *
     FROM
         (SELECT hapar_id,
                 year,
                 string_agg(land_leased_out :: VARCHAR, '')
          FROM temp_seasonal
          GROUP BY hapar_id,
                   year) foo
     WHERE string_agg LIKE '%Y%')
SELECT *
FROM temp_seasonal AS s
WHERE hapar_id NOT IN
        (SELECT DISTINCT hapar_id
         FROM temp_permanent)
    AND hapar_id IN
        (SELECT hapar_id
         FROM sub);
	
SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       year,
       sum(bps_claimed_area),
       string_agg(land_leased_out :: VARCHAR, '')
FROM switch_to_perm
GROUP BY mlc_hahol_id,
         habus_id,
         hahol_id,
         hapar_id,
         year


SELECT *
FROM temp_seasonal AS s
WHERE hapar_id NOT IN
        (SELECT DISTINCT hapar_id
         FROM temp_permanent)
    AND hapar_id IN
        (SELECT hapar_id
         FROM
             (SELECT hapar_id,
                     year,
                     string_agg(land_leased_out :: VARCHAR, '')
              FROM temp_seasonal
              GROUP BY hapar_id,
                       year) foo
         WHERE string_agg LIKE '%Y%')


SELECT mlc_hahol_id,
       habus_id,
       hahol_id,
       hapar_id,
       year,
       sum(bps_claimed_area) AS sum_bps,
       string_agg(land_leased_out :: VARCHAR, '')
FROM
    (SELECT *
     FROM temp_seasonal AS s
     WHERE hapar_id IN
             (SELECT DISTINCT hapar_id
              FROM temp_permanent)
         AND hapar_id IN
             (SELECT hapar_id
              FROM
                  (SELECT hapar_id,
                          year,
                          string_agg(land_leased_out :: VARCHAR, '')
                   FROM temp_seasonal
                   GROUP BY hapar_id,
                            year) foo
              WHERE string_agg LIKE '%Y%'))
GROUP BY mlc_hahol_id,
         habus_id,
         hahol_id,
         hapar_id,
         year


--PRELIM JOIN for main tables
--! problem: this leaves out potential claims from same hapar_id
--so make into temp table and check if hapar_id exists in old table
SELECT p.mlc_hahol_id AS owner_mlc_hahol_id,
       s.mlc_hahol_id AS user_mlc_hahol_id,
       p.habus_id AS owner_habus_id,
       s.habus_id AS user_habus_id,
       p.hahol_id AS owner_hahol_id,
       s.hahol_id AS user_hahol_id,
       hapar_id,
       land_parcel_area,
       bps_eligible_area,
       s.bps_claimed_area,
       verified_exclusion,
       land_use_area,
       p.land_use AS owner_land_use,
       s.land_use AS user_land_use,
       s.land_activity,
       s.application_status,
       'Y' AS land_leased_out,
       s.lfass_flag,
       claim_id_p,
       claim_id_s,
       year,
       CONCAT(p.change_note, s.change_note) AS change_note
FROM temp_permanent AS p
JOIN temp_seasonal AS s USING (hapar_id,
                               land_parcel_area,
                               bps_eligible_area,
                               verified_exclusion,
                               land_use_area,
                               year)
WHERE p.land_leased_out = 'Y'
    AND s.land_leased_out = 'N'
ORDER BY change_note DESC; --26,086 combined rows


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




--mark owners in seasonal table by LLO flag 
UPDATE temp_seasonal
SET is_perm_flag = 'Y',
    claim_id_s = 'P' || TRIM('S'
                             from claim_id_s) || '_01',
    change_note = CONCAT(change_note, 'P record moved from seasonal to permanent sheet; ')
WHERE land_leased_out = 'Y'
    AND claim_id_s NOT LIKE '%P%'; --updates 1,432 rows



--separate claims on parcels (which exist in both p and s sheets but) which are only claimed by one party for one year
--      [join where hapar_id match but year doesn't, and NOT IN join using (hapar_id, year)]
DROP TABLE IF EXISTS p_only;
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
WHERE t.claim_id_p = combine.claim_id; -- moves 39,567 rows

DROP TABLE IF EXISTS s_only;
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
WHERE t.claim_id_s = combine.claim_id; --moves 16,764 rows to combine table

--delete land_use_area IS NULL 
DELETE FROM temp_permanent 
WHERE land_use_area IS NULL; --removes 353 rows

DELETE FROM temp_seasonal 
WHERE land_use_area IS NULL; --removes 1,187 rows

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

 --------------------------------------------------
-- Finds single land use claims per hapar_id and year

SELECT *
FROM
    (SELECT hapar_id,
            year,
            COUNT(Distinct land_use) as dupes,
            COUNT(land_use) as lu_count,
            SUM(land_use_area) as sum_lu
     FROM temp_permanent
     GROUP BY hapar_id,
              year) foo
WHERE dupes <> lu_count --------------------------------------------------
-- LOW PRIORITY land_leased_out records flagged YES in Seasonal table repeated with different habus_ids

    SELECT seas.habus_id,
           llo.habus_id,
           seas.hahol_id,
           llo.hahol_id,
           seas.hapar_id,
           llo.hapar_id,
           seas.land_use,
           llo.land_use,
           seas.land_use_area,
           llo.land_use_area,
           seas.land_leased_out,
           llo.land_leased_out
    FROM temp_seasonal as seas
    INNER JOIN
        (SELECT *
         FROM temp_seasonal
         WHERE land_leased_out = 'Y') AS llo ON seas.hapar_id = llo.hapar_id
    AND seas.year = llo.year
    AND seas.habus_id <> llo.habus_id; -- returns 946 records but there are 1,386 total records flagged YES in seasonal f_table_catalog (120 missing)

--------------------------------------------------------------------------
--TODO      MIS-match original perms to spatial layer for year 2018

WITH subq AS
    (SELECT hapar_id,
            year,
            sum(land_use_area)
     FROM rpid.saf_permanent_land_parcels_deliv20190911
     WHERE land_use_area <> 0
         AND year = 2018
     GROUP BY hapar_id,
              year)
SELECT *
FROM rpid.lpis_land_parcels_2019_jhi_deliv20190911 AS spat
INNER JOIN subq ON spat.hapar_id = subq.hapar_id
WHERE ROUND(spat.digi_area, 2) <> subq.sum --TODO      -------------------------------------------------------

-- Finds most of the multiple claims on same-year seasonal parcels
-- waiting for answer from Allen at RPID ...

    SELECT *
    FROM
        (SELECT hapar_id,
                land_parcel_area,
                sum(land_use_area) OVER(PARTITION BY hapar_id, year) AS sum_lua,
                sum(bps_claimed_area) OVER(PARTITION BY hapar_id, year) AS sum_bps_e,
                year
         FROM temp_seasonal) foo WHERE land_parcel_area < sum_lua --  AND land_parcel_area < sum_bps_e

    SELECT *
    FROM
        (SELECT hapar_id,
                land_parcel_area,
                sum(land_use_area) OVER (PARTITION BY hapar_id,
                                                      land_parcel_area,
                                                      year) AS sum_lua,
                                        YEAR
         FROM temp_permanent) foo WHERE land_parcel_area = sum_lua --TODO      Find all the records in seasonal sheet that need to be moves to permanent sheet with LLO switched to yes

    SELECT *
    FROM
        (SELECT hapar_id,
                COUNT(hapar_id)
         FROM
             (SELECT hapar_id,
                     land_parcel_area,
                     year
              FROM temp_permanent
              GROUP BY hapar_id,
                       land_parcel_area,
                       year) foo
         GROUP BY hapar_id) foo WHERE COUNT > 1
ORDER BY count DESC
SELECT *
FROM
    (SELECT mlc_hahol_id,
            habus_id,
            hapar_id,
            year,
            string_agg(land_leased_out :: text, '')
     FROM temp_seasonal
     GROUP BY mlc_hahol_id,
              habus_id,
              hapar_id,
              year) foo
WHERE string_agg LIKE '%Y%'
    SELECT *
    FROM temp_seasonal Where hapar_id = 393938
UNION
SELECT *
FROM temp_permanent
WHERE hapar_id = 393938
ORDER BY year,
         is_perm_flag,
         land_use,
         habus_id 


/*n. --FIXED-- Single lonely claims occuring only once in all tables */ -- changes is_perm_flag to N for seasonal claims found in permanent table

    UPDATE temp_permanent AS t
    SET is_perm_flag = 'N',
        change_note = CONCAT(t.change_note, 'is_perm_flag changed to N; '),
        claim_id_p = CONCAT('s', t.claim_id_p)
    FROM temp_permanent
    JOIN
        (SELECT hapar_id,
                year
         FROM
             (SELECT hapar_id,
                     YEAR,
                     land_parcel_area/sum_lua AS per_right
              FROM
                  (SELECT hapar_id,
                          YEAR,
                          land_parcel_area,
                          sum(land_use_area) OVER (PARTITION BY hapar_id,
                                                                year) AS sum_lua
                   FROM temp_permanent) foo
              WHERE land_parcel_area < sum_lua) foo2
         GROUP BY hapar_id,
                  YEAR,
                  per_right
         HAVING per_right = 0.5) sub USING (hapar_id,
                                            year) WHERE t.hapar_id = sub.hapar_id
    AND t.YEAR = sub.YEAR
    AND t.land_leased_out = 'N'; --updates 51 rows         



-- below is a duplicate

SELECT *
FROM temp_seasonal
WHERE hapar_id = 211520
ORDER BY year --! check this bit out -- there's more seasonal than permanent */    