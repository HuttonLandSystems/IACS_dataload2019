--TODO   should I make all excl land_use match verified exclusion unless where separated? (about 7k in perm)
--TODO   look at LFASS flag where claim = 0

--* finds and sums difference in overclaims where bps_claimed_area > land_parcel_area for perm and seas table 
SELECT sum(land_parcel_area - bps_claimed_area)
FROM
    (SELECT hapar_id,
            year
     FROM
         (SELECT hapar_id,
                 YEAR,
                 sum_bps,
                 land_parcel_area
          FROM
              (SELECT hapar_id,
                      YEAR,
                      sum(bps_claimed_area) AS sum_bps
               FROM temp_seasonal
               GROUP BY hapar_id,
                        YEAR) foo
          JOIN temp_seasonal USING (hapar_id,
                                     YEAR)) bar
     WHERE sum_bps > land_parcel_area
     GROUP BY hapar_id,
              year) foobar
JOIN temp_seasonal USING (hapar_id,
                           year);

--* finds and sums difference in overclaims where bps_claimed_area > land_parcel_area for joined table
SELECT sum(total_claimed_area - owner_land_parcel_area)
FROM
    (SELECT hapar_id,
            YEAR,
            owner_land_parcel_area,
            user_land_parcel_area,
            owner_bps_claimed_area + user_bps_claimed_area AS total_claimed_area
     FROM joined) foo
WHERE total_claimed_area > owner_land_parcel_area
    OR total_claimed_area > user_land_parcel_area;

SELECT * 
FROM temp_permanent
WHERE hapar_id = 40016
ORDER BY year
--* finds multiple businesses for same hapar, year
SELECT *
FROM
    (SELECT habus_id,
            hapar_id,
            YEAR,
            ROW_NUMBER() OVER (PARTITION BY hapar_id,
                                            YEAR)
     FROM temp_seasonal
     GROUP BY habus_id,
              hapar_id,
              YEAR) foo
WHERE ROW_NUMBER > 1

--! need to combine same land_use year hapar_id -- no i dont because they're separated for a reason !
WITH same_lu AS (
    SELECT *
    FROM
        ( SELECT mlc_hahol_id,
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
                change_note,
                ROW_NUMBER() OVER (PARTITION BY hapar_id,
                                                land_use,
                                                year, 
                                                land_leased_out)
        FROM temp_permanent ) foo
    WHERE ROW_NUMBER > 1
)

-- run all the way up to join 
-- check hapars 1135,1144,1146,1725,1728
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
           change_note, 
           ROW_NUMBER() OVER (PARTITION BY hapar_id, land_use, year)
    FROM temp_permanent
    WHERE hapar_id > 1100
ORDER BY hapar_id, year



SELECT * 
FROM temp_seasonal
WHERE verified_exclusion <> land_use_area AND land_use IN (SELECT land_use FROM excl)
ORDER BY hapar_id, year

--! Ask Doug
SELECT *
FROM FINAL
WHERE owner_land_use IN
        (SELECT land_use
         FROM excl)
    AND user_land_use IN
        (SELECT land_use
         FROM excl)
    AND user_land_use_area <> 0
    AND user_land_use <> 'EXCL'
    -- especially hapar_id = 369777 -- this one has matching hahol id
    -- with 369777 owner PGRS needs to be subdivided into PGRS and RGR
    -- the problem happens when I do second join and then delete all joined from original. I need to be able to join something again but how
    -- SO THE TRICK IS NOT TO DELETE BUT DO JOINS ANYWAY AND DELETE DUPLICATES AFTERWARD


--! Do everything by FID and not claim
--! FID level analysis, no claim level
--TODO      compare owner_bps_eligible_area and user_bps_eligible_area

--TODO check claimed_area vs land_parcel-area

--!weird case with 36 ha of BUILDING and no matching land_uses
SELECT * 
FROM rpid.saf_permanent_land_parcels_deliv20190911 splpd
WHERE hapar_id = 212811  AND YEAR = 2017
UNION 
SELECT * 
FROM rpid.saf_seasonal_land_parcels_deliv20190911 sslpd
WHERE hapar_id = 212811 AND YEAR = 2017
ORDER BY YEAR, is_perm_flag

--!last checks 
SELECT hapar_id,
owner_verified_exclusion,
user_verified_exclusion,
       CASE
           WHEN owner_verified_exclusion > user_verified_exclusion THEN owner_verified_exclusion - user_verified_exclusion
           WHEN user_verified_exclusion > owner_verified_exclusion THEN user_verified_exclusion - owner_verified_exclusion
           WHEN owner_verified_exclusion = owner_land_use_area THEN 0 
           WHEN user_verified_exclusion = user_land_use_area THEN 0 
           ELSE 9999999999           
       END AS verified_exclusion_diff,
       change_note
FROM joined
WHERE owner_verified_exclusion <> user_verified_exclusion
ORDER BY verified_exclusion_diff DESC

SELECT hapar_id, 
owner_land_activity, 
user_land_activity
FROM joined 
WHERE owner_land_activity <> user_land_activity

--TODO   last checks      compare owner_land_parcel_area and user_land_parcel_area
SELECT hapar_id,
       sum_owner,
       sum_user,
       CASE
           WHEN sum_owner > sum_user THEN sum_owner - sum_user
           WHEN sum_user > sum_owner THEN sum_user - sum_owner
       END AS diff,
       year
FROM
    (SELECT hapar_id,
            year,
            sum(owner_land_parcel_area) AS sum_owner,
            sum(user_land_parcel_area) AS sum_user
     FROM joined
     GROUP BY hapar_id, year) foo
WHERE sum_owner <> sum_user
ORDER BY diff DESC

--TODO   last checks      compare owner_bps_eligible_area and user_bps_eligible_area
SELECT hapar_id,
       sum_owner,
       sum_user,
       CASE
           WHEN sum_owner > sum_user THEN sum_owner - sum_user
           WHEN sum_user > sum_owner THEN sum_user - sum_owner
       END AS diff,
       year
FROM
    (SELECT hapar_id,
            year,
            sum(owner_bps_eligible_area) AS sum_owner,
            sum(user_bps_eligible_area) AS sum_user
     FROM joined
     GROUP BY hapar_id, year) foo
WHERE sum_owner <> sum_user
ORDER BY diff DESC

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
    (SELECT hapar_id, 
            year, 
            COUNT(Distinct land_use) as dupes, 
            COUNT(land_use) as lu_count, 
            SUM(land_use_area) as sum_lu 
     FROM temp_seasonal 
     GROUP BY hapar_id, 
              year) foo 
WHERE dupes <> lu_count 
ORDER BY lu_count DESC, 
         dupes DESC 
SELECT mlc_hahol_id, 
       habus_id, 
       hahol_id, 
       hapar_id, 
       land_use, 
       year, 
       SUM(land_use_area) OVER (PARTITION BY mlc_hahol_id, 
                                             habus_id, 
                                             hahol_id, 
                                             land_use, 
                                             year)
FROM temp_seasonal 
WHERE hapar_id = 85859
    AND year = 2016 
ORDER BY land_use, 
         habus_id /* THIS ONE IS WEIRD sum(land_use_area) is consistently bigger than parcel area - is someone owning/renting?
*/ 


SELECT * 
FROM combine 
WHERE owner_bps_claimed_area > owner_bps_eligible_area AND owner_bps_claimed_area > owner_land_parcel_area
ORDER BY hapar_id -- Correct bps_claimed_area > land_parcel_area claims?




SELECT *
FROM
    (SELECT hapar_id,
            year,
            (owner_land_parcel_area/owner_land_use_area) AS percent_right
     FROM combine
     WHERE owner_land_use_area > owner_land_parcel_area) foo
WHERE percent_right < 0.95
ORDER BY percent_right --! Weird cases need looking ---------------------------------------------

SELECT * 
FROM combine
WHERE hapar_id = 71082
ORDER BY year --TODO this one has wrong bps_claimed_area and 122913, 335946 ,190255

SELECT *
FROM rpid.saf_permanent_land_parcels_deliv20190911 splpd
WHERE hapar_id = 1146
    AND YEAR <> 2019
UNION
SELECT *
FROM rpid.saf_seasonal_land_parcels_deliv20190911 sslpd
WHERE hapar_id = 1146
    AND YEAR <> 2019
ORDER BY YEAR,
         is_perm_flag,
         habus_id -- particular hapar_id where different land_use and in one case different land_use_area

SELECT *
FROM temp_seasonal
WHERE hapar_id = 438015
UNION
SELECT *
FROM temp_permanent
WHERE hapar_id = 438015
ORDER BY year



SELECT * 
FROM rpid.saf_permanent_land_parcels_deliv20190911 
WHERE hapar_id = 295139 AND YEAR = 2016
UNION 
SELECT * 
FROM rpid.saf_seasonal_land_parcels_deliv20190911 
WHERE hapar_id = 295139 AND YEAR = 2016
ORDER BY habus_id

--This code checks to see if land_parcel_area is the same for each year
SELECT *
FROM
    (SELECT hapar_id,
            YEAR,
            row_number() OVER (PARTITION BY hapar_id,
                                            YEAR,
                                            user_land_parcel_area)
     FROM
         (SELECT hapar_id,
                 YEAR,
                 user_land_parcel_area
          FROM joined
          GROUP BY hapar_id,
                   YEAR,
                   user_land_parcel_area) foo) foo2
WHERE ROW_NUMBER > 1