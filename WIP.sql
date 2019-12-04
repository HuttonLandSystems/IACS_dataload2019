--! Do everything by FID and not claim
--! FID level analysis, no claim level
--TODO      compare owner_land_parcel_area and user_land_parcel_area
--TODO      compare owner_bps_eligible_area and user_bps_eligible_area

--TODO check claimed_area vs land_parcel-area

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