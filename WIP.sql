--! Do everything by FID and not claim
--! FID level analysis, no claim level


--TODO      compare owner_land_parcel_area and user_land_parcel_area 
--TODO      compare owner_bps_eligible_area and user_bps_eligible_area 


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
WHERE dupes <> lu_count
--------------------------------------------------
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
    ( SELECT hapar_id,
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
WHERE ROUND(spat.digi_area, 2) <> subq.sum
--TODO      -------------------------------------------------------
-- Finds most of the multiple claims on same-year seasonal parcels
-- waiting for answer from Allen at RPID ...
SELECT *
FROM
    (SELECT hapar_id,
            land_parcel_area,
            sum(land_use_area) OVER(PARTITION BY hapar_id, year) AS sum_lua,
            sum(bps_claimed_area) OVER(PARTITION BY hapar_id, year) AS sum_bps_e,
            year
     FROM temp_seasonal) foo
WHERE land_parcel_area < sum_lua
  --  AND land_parcel_area < sum_bps_e


SELECT *
FROM
    (SELECT hapar_id,
            land_parcel_area,
            sum(land_use_area) OVER (PARTITION BY hapar_id,
                                                  land_parcel_area,
                                                  year) AS sum_lua,
                                    YEAR
     FROM temp_permanent) foo
WHERE land_parcel_area = sum_lua


--TODO      Find all the records in seasonal sheet that need to be moves to permanent sheet with LLO switched to yes

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
     GROUP BY hapar_id) foo
WHERE COUNT > 1
ORDER BY count DESC


SELECT * 
FROM (
SELECT mlc_hahol_id,
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
FROM temp_seasonal
Where hapar_id = 393938
UNION
SELECT * 
FROM temp_permanent
WHERE hapar_id = 393938
ORDER BY year, is_perm_flag, land_use, habus_id

/* Example problems:
1. owners in seasonal table, renters in permanent table             154126, 294355, 422426, 125286, 77527, 607372, 578256, 421990, 405608
2. Same land_use_claims in seasonal table associated with different businesses - only one with bps_claimed_area 0 and another with LLO yes          381266, 591113, 99299, 590132
        GROUP BY mlc_hahol_id and habus_id and then move the one with sum(bps_claimed_area) = 0 to the permanent sheet and change is_perm_flag and CONCAT (claim_id_s to something else)
3. Different land_use claims in seasonal table associated with different businesses - only with bps_claimed_area 0 and another with LLO yes         590224, 388042
        Need different owner_land_use and CONCAT(user_land_use)
4. Two businesses renting out the same land to three other businesses where bps_claimed_area is much less than land_use_area                          39194, 39273
5. sum_bpc_claimed_area > land_parcel_area 
6. Owners and renters in same table (temp_permanent) and (temp_seasonal)*/

SELECT *
FROM
    (SELECT mlc_hahol_id,
            habus_id,
            hapar_id,
            year,
            land_parcel_area,
            sum(bps_claimed_area) AS sum_bps
     FROM temp_permanent
     GROUP BY mlc_hahol_id,
              habus_id,
              hapar_id,
              land_parcel_area,
              year) foo
WHERE land_parcel_area < sum_bps

/*
n. --FIXED-- Single lonely claims occuring only once in all tables */





-- changes is_perm_flag to N for seasonal claims found in permanent table
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
                                        year)
WHERE t.hapar_id = sub.hapar_id
    AND t.YEAR = sub.YEAR
    AND t.land_leased_out = 'N'; --updates 51 rows





-- below is a duplicate
SELECT * FROM temp_seasonal WHERE hapar_id = 211520 ORDER BY year 

--! check this bit out -- there's more seasonal than permanent */
SELECT *
FROM
    (SELECT hapar_id,
            year,
            COUNT(Distinct land_use) as dupes,
            COUNT(land_use) as lu_count,
            SUM(land_use_area) as sum_lu
     FROM temp_seasonal
     GROUP BY hapar_id,
              year) foo
              WHERE dupes <> lu_count
              ORDER BY lu_count DESC, dupes DESC
 
SELECT mlc_hahol_id, habus_id, hahol_id, hapar_id,
       land_use,
       year,
       SUM(land_use_area) OVER (PARTITION BY mlc_hahol_id,
                                             habus_id,
                                             hahol_id,
                                             land_use,
                                             year)
FROM temp_seasonal
WHERE hapar_id = 85859 AND year = 2016
ORDER BY land_use, habus_id

/* THIS ONE IS WEIRD
SELECT * 
FROM temp_permanent
WHERE hapar_id = 472985 has two imputed records ! WHY. because hahol_id is null and doesnt match - not related
*/

WITH sub1 AS
    (SELECT *
     FROM temp_permanent
     WHERE land_use = 'IMPUTED_RECORD'),
     sub2 AS
    (SELECT *
     FROM temp_permanent
     WHERE land_use_area = 0)
SELECT *
FROM sub2
INNER JOIN sub1 USING (mlc_hahol_id,
                       habus_id,
                       hahol_id,
                       hapar_id,
                       land_parcel_area,
                       year); -- 178 rows
--! default to RGR over PGRS?

--JOIN -- but how to ensure whole hapar_id or nothing?!
SELECT p.mlc_hahol_id,
       s.mlc_hahol_id,
       p.habus_id,
       s.habus_id,
       p.hahol_id,
       s.hahol_id,
       hapar_id,
       p.land_parcel_area,
       s.land_parcel_area,
       p.bps_eligible_area,
       s.bps_eligible_area,
       p.bps_claimed_area,
       s.bps_claimed_area,
       p.verified_exclusion,
       s.verified_exclusion,
       p.land_use_area,
       s.land_use_area,
       p.land_use,
       s.land_use,
       p.land_activity,
       s.land_activity,
       p.application_status,
       s.application_status,
       'Y' AS land_leased_out,
       p.lfass_flag,
       s.lfass_flag,
       CONCAT(claim_id_p, '; ', claim_id_s) AS claim_id,
       year,
       CASE
           WHEN p.change_note IS NULL
                OR s.change_note IS NULL THEN CONCAT(p.change_note, s.change_note)
           ELSE CONCAT(p.change_note, '; ', s.change_note)
       END AS change_note
FROM temp_permanent p
JOIN temp_seasonal s USING (hapar_id,
year)
ORDER BY hapar_id, year;
        
/* CASE
       WHEN p.land_activity = ''
            OR s.land_activity = '' THEN CONCAT (p.land_activity, ', ', s.land_activity)
       ELSE s.land_activity
   END AS land_activity,*/
    
-- TODO         bps_claimed_area 
SELECT * 
FROM temp_permanent
WHERE bps_claimed_area > bps_eligible_area

--Step 2d. Correct bps_claimed_area > land_parcel_area claims?
SELECT *
FROM
    (SELECT mlc_hahol_id,
            habus_id,
            hahol_id,
            hapar_id,
            year,
            land_parcel_area,
            sum(bps_claimed_area) AS sum_bps
     FROM temp_seasonal
     GROUP BY mlc_hahol_id,
              habus_id,
              hahol_id,
              hapar_id,
              land_parcel_area,
              year) foo
WHERE land_parcel_area < sum_bps

--! check percentages wrong
SELECT *
FROM
    (SELECT hapar_id,
            year,
            (land_parcel_area/land_use_area) AS percent_right
     FROM singles_p
     WHERE land_use_area > land_parcel_area) foo
WHERE percent_right < 0.95
ORDER BY percent_right

--! Weird cases need looking ---------------------------------------------
SELECT * 
FROM rpid.saf_permanent_land_parcels_deliv20190911 splpd
WHERE hapar_id = 1146 AND YEAR <> 2019
UNION 
SELECT * 
FROM rpid.saf_seasonal_land_parcels_deliv20190911 sslpd 
WHERE hapar_id = 1146 AND YEAR <> 2019
ORDER BY YEAR, is_perm_flag, habus_id

-- particular hapar_id where different land_use and in one case different land_use_area
SELECT * 
FROM temp_seasonal
WHERE hapar_id = 438015
UNION 
SELECT * 
FROM temp_permanent 
WHERE hapar_id = 438015
ORDER BY year 


UPDATE test_join
SET owner_mlc_hahol_id = user_mlc_hahol_id,
    user_mlc_hahol_id = owner_mlc_hahol_id,
    owner_habus_id = user_habus_id,
    user_habus_id = owner_habus_id,
    owner_hahol_id = user_hahol_id,
    user_hahol_id = owner_hahol_id,
    owner_land_parcel_area = user_land_parcel_area,
    user_land_parcel_area = owner_land_parcel_area,
    owner_bps_eligible_area = user_bps_eligible_area,
    user_bps_eligible_area = owner_bps_eligible_area,
    owner_bps_claimed_area = user_bps_claimed_area,
    user_bps_claimed_area = owner_bps_claimed_area,
    owner_verified_exclusion = user_verified_exclusion,
    user_verified_exclusion = owner_verified_exclusion,
    owner_land_use_area = user_land_use_area,
    user_land_use_area = owner_land_use_area,
    owner_land_use = user_land_use,
    user_land_use = owner_land_use,
    owner_land_activity = user_land_activity,
    user_land_activity = owner_land_activity,
    owner_application_status = user_application_status,
    user_application_status = owner_application_status,
    land_leased_out = 'Y',
    owner_lfass_flag = user_lfass_flag,
    user_lfass_flag = owner_lfass_flag,
    change_note = CONCAT(change_note, 'swapped owner/renter; ')
WHERE change_note LIKE '%first%'
    AND owner_bps_claimed_area <> 0
    AND user_bps_claimed_area = 0

--*STEP 4. Find renter records in wrong tables 
--finds multiple businesses claiming on same land in permanent table and marks them as seasonal, and vice versa

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
UPDATE temp_permanent AS t
SET land_leased_out = (CASE
                           WHEN t.land_use <> 'EXCL' THEN 'Y'
                           ELSE t.land_leased_out
                       END),
    claim_id_p = 'S' || TRIM('P'
                             FROM t.claim_id_p) || '_01',
    is_perm_flag = 'N',
    change_note = CONCAT(t.change_note, 'S record moved from permanent to seasonal sheet; ')
FROM find_switches AS a
JOIN temp_permanent AS b USING (hapar_id,
                                year)
WHERE t.hapar_id = a.hapar_id
    AND t.year = a.year; --6,214 rows

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
                             from t.claim_id_s) || '_01',
    is_perm_flag = 'Y',
    change_note = CONCAT(t.change_note, 'P record moved from seasonal to permanent sheet; ')  
FROM find_switches AS a 
JOIN temp_seasonal AS b USING (hapar_id, year)
WHERE t.hapar_id = a.hapar_id AND 
t.year = a.year; --  6,231 rows  