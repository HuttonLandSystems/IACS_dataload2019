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