-- categorises parcel area vs land use area
SELECT CASE
           WHEN land_parcel_area > sum_lua THEN 'Parcel > LU'
           WHEN land_parcel_area = sum_lua THEN 'Equal'
           WHEN land_parcel_area < sum_lua THEN 'Parcel < LU'
           ELSE 'Other'
       END AS Area_comparison,
       COUNT(1)
FROM
     (SELECT hapar_id,
             SUM(land_use_area) OVER (PARTITION BY hapar_id,
                                                   year) AS sum_lua,
                                     land_parcel_area,
                                     year
      FROM temp_permanent) foo
GROUP BY 1
ORDER BY count DESC;

Equal	     1,542,209
Parcel > LU	349,954
Parcel < LU	6,112

(10809/1959847)*100 = 0.55% <-- permanent

Equal	     127,451
Parcel > LU	39,547
Parcel < LU	5,085

(5440/180233)*100 = 3.01% <-- seasonal

--* finds and sums difference in SINGLE overclaims where bps_claimed_area > land_parcel_area for joined table
SELECT hapar_id, YEAR, total_claimed_area - owner_land_parcel_area AS diff
FROM
    (SELECT hapar_id,
            YEAR,
            owner_land_parcel_area,
            user_land_parcel_area,
            owner_bps_claimed_area + user_bps_claimed_area AS total_claimed_area
     FROM joined) foo
WHERE total_claimed_area > owner_land_parcel_area
    OR total_claimed_area > user_land_parcel_area
    ORDER BY diff DESC;


 -- categorises llo flags based on matches or no matches
SELECT CASE
           WHEN perm_llo = 'N'
                AND seas_llo = 'Y' Then 'N-Y no match'
           WHEN perm_llo = 'Y'
                AND seas_llo = 'N' THEN 'Y-N no match'
           WHEN perm_llo = 'N'
                AND seas_llo = 'N' THEN 'N match'
           WHEN perm_llo = 'Y'
                AND seas_llo = 'Y' THEN 'Y match'
           WHEN perm_llo = seas_llo THEN 'Match'
           ELSE 'Other'
       END as LLO_flag_compare,
       Count(1)
FROM
     (SELECT hapar_id,
             year,
             perm.land_leased_out AS perm_llo,
             seas.land_leased_out AS seas_llo
      FROM temp_permanent AS perm
      INNER JOIN temp_seasonal AS seas USING (hapar_id,
                                              land_use,
                                              land_use_area,
                                              year)
      GROUP BY hapar_id,
               year,
               perm_llo,
               seas_llo) foo
GROUP BY 1
ORDER BY LLO_flag_compare
 
-- Where total land use area matches but number of claims doesnt ON JOIN
SELECT DISTINCT perm_s.hapar_id
FROM (
           ( SELECT hapar_id,
                    SUM(land_use_area) lua_per,
                    COUNT(*) AS cnt_per,
                    land_parcel_area
            FROM temp_permanent
            WHERE YEAR = 2018
            GROUP BY hapar_id,
                     land_parcel_area) perm_s
      INNER JOIN
           ( SELECT hapar_id,
                    SUM(land_use_area) lua_sea,
                    COUNT(*) AS cnt_sea,
                    land_parcel_area
            FROM temp_seasonal
            WHERE YEAR = 2018
            GROUP BY hapar_id,
                     land_parcel_area) seas_s ON seas_s.hapar_id = perm_s.hapar_id
      AND lua_sea = lua_per)
WHERE cnt_per != cnt_sea

--See changes made by count
SELECT change_note,
       COUNT(*)
FROM combine
GROUP BY change_note
ORDER BY count DESC


All code moved to removed.sql 

--TODO  This code finds digi_area used in join aka proportion of good area for one year
-- 5,123,636/6,479,357 hectares = 79.0% of 2017 parcels (area)
SELECT SUM(digi_area) FROM
    (SELECT hapar_id, digi_area
     FROM ladss.snapshot_2017
     JOIN final AS l USING (hapar_id)
     WHERE l.YEAR = 2016
     GROUP BY hapar_id, digi_area) foo

-- 5,129,263/6,476,784 hectares = 79.2% of 2018 parcels (area)
SELECT SUM(digi_area) FROM
    (SELECT hapar_id, digi_area
     FROM ladss.snapshot_2018
     JOIN final AS l USING (hapar_id)
     WHERE l.YEAR = 2017
     GROUP BY hapar_id, digi_area) foo

-- 5,073,818/6,479,879 hectares = 78.3% of 2019 parcels (area)
SELECT SUM(digi_area) FROM
    (SELECT hapar_id, digi_area
     FROM ladss.snapshot_2019
     JOIN final AS l USING (hapar_id)
     WHERE l.YEAR = 2018
     GROUP BY hapar_id, digi_area) foo

--* ORIGINAL DATA   
-- 5,213,085/6,479,357 hectares = 80.5% of 2017 parcels (area)
SELECT SUM(digi_area)
FROM
    (SELECT hapar_id,
            digi_area
     FROM ladss.snapshot_2017
     JOIN
         (SELECT DISTINCT hapar_id
          FROM
              (SELECT DISTINCT hapar_id
               FROM rpid.saf_permanent_land_parcels_deliv20190911
               WHERE YEAR = 2016
               UNION SELECT DISTINCT hapar_id
               FROM rpid.saf_seasonal_land_parcels_deliv20190911
               WHERE YEAR = 2016) foo) bar USING (hapar_id)
     GROUP BY hapar_id,
              digi_area) foobar

-- 5,236,722/6,476,784 hectares = 80.9% of 2018 parcels (area)
SELECT SUM(digi_area)
FROM
    (SELECT hapar_id,
            digi_area
     FROM rpid.lpis_land_parcels_2018_jhi_deliv20190911
     JOIN
         (SELECT DISTINCT hapar_id
          FROM
              (SELECT DISTINCT hapar_id
               FROM rpid.saf_permanent_land_parcels_deliv20190911
               WHERE YEAR = 2017
               UNION SELECT DISTINCT hapar_id
               FROM rpid.saf_seasonal_land_parcels_deliv20190911
               WHERE YEAR = 2017) foo) bar USING (hapar_id)
     GROUP BY hapar_id,
              digi_area) foobar

-- 5,226,963/6,479,879 hectares = 80.7% of 2019 parcels (area)
SELECT SUM(digi_area)
FROM
    (SELECT hapar_id,
            digi_area
     FROM rpid.lpis_land_parcels_2019_jhi_deliv20190911
     JOIN
         (SELECT DISTINCT hapar_id
          FROM
              (SELECT DISTINCT hapar_id
               FROM rpid.saf_permanent_land_parcels_deliv20190911
               WHERE YEAR = 2018
               UNION SELECT DISTINCT hapar_id
               FROM rpid.saf_seasonal_land_parcels_deliv20190911
               WHERE YEAR = 2018) foo) bar USING (hapar_id)
     GROUP BY hapar_id,
              digi_area) foobar

--  5,123,636/5,213,085 = 98.3% 2017
--  5,130,084/5,236,722 = 98.0% 2018
--  5,074,587/5,226,963 = 97.1% 2019     

--TODO how many have at least one owner or user 
-- 445,203/522,743 = 85.2% of 2017 parcels have at least one claim associated
-- 444,518‬/525,621 = 84.6% of 2018 parcels have at least one claim associated 
-- 440,026‬/526,899 = 83.5% of 2019 parcels have at least one claim associated 
SELECT hapar_id
FROM rpid.lpis_land_parcels_2019_jhi_deliv20190911
EXCEPT
SELECT hapar_id
FROM final AS l
WHERE l.YEAR = 2018

-- ORIGINAL DATA     

-- 448,840/522,743 = 85.9% of original data 
-- 449,556‬/525,621 = 85.5% of original data
-- 451,774/526,899 = 85.7% of original data 
SELECT hapar_id
FROM rpid.lpis_land_parcels_2018_jhi_deliv20190911
EXCEPT
    (SELECT hapar_id
     FROM rpid.saf_permanent_land_parcels_deliv20190911
     WHERE YEAR = 2017
     UNION SELECT hapar_id
     FROM rpid.saf_seasonal_land_parcels_deliv20190911
     WHERE YEAR = 2017)

-- 445,203/448,840 = 99.2%
-- 444,518‬/449,556‬ = 98.9%
-- 440,026/451,774 = 97.4%

--TODO how many time does digi_area match land_parcel_area within 0.1 and 0.01 threshold 
SELECT CASE
           WHEN diff < 0.01 THEN '< 0.01 difference'
           WHEN diff > 0.01
                AND diff < 0.1 THEN '< 0.1 difference'
           ELSE '> 0.1 difference'
       END AS digi_area_match,
       COUNT(1)
FROM
    (SELECT hapar_id,
            land_parcel_area,
            digi_area,
            ABS(land_parcel_area - digi_area) AS diff
     FROM
         (SELECT hapar_id,
                 land_parcel_area
          FROM ladss.saf_iacs_2016_2017_2018
          WHERE YEAR = 2016
          GROUP BY hapar_id,
                   land_parcel_area) foo
     JOIN ladss.snapshot_2017 USING (hapar_id)) bar
GROUP BY 1
ORDER BY count DESC

-- 2018
SELECT CASE
           WHEN diff < 0.01 THEN '< 0.01 difference'
           WHEN diff > 0.01
                AND diff < 0.1 THEN '< 0.1 difference'
           ELSE '> 0.1 difference'
       END AS digi_area_match,
       COUNT(1)
FROM
    (SELECT hapar_id,
            land_parcel_area,
            digi_area,
            ABS(land_parcel_area - digi_area) AS diff
     FROM
         (SELECT hapar_id,
                 land_parcel_area
          FROM ladss.saf_iacs_2016_2017_2018
          WHERE YEAR = 2017
          GROUP BY hapar_id,
                   land_parcel_area) foo
     JOIN ladss.snapshot_2018 USING (hapar_id)) bar
GROUP BY 1
ORDER BY count DESC

--TODO count of user businesses map
--TODO underclaims / overclaims
SELECT hapar_id,
       YEAR,
       sum,
       saf.land_parcel_area,
       ABS(sum - saf.land_parcel_area)
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
          FROM ladss.saf_iacs_2016_2017_2018) foo
     GROUP BY hapar_id,
              YEAR) bar
JOIN ladss.saf_iacs_2016_2017_2018 USING (hapar_id,
                                          year)
JOIN ladss.saf_iacs_2016_2017_2018 AS saf USING (hapar_id,
                                                 year)
WHERE sum > saf.land_parcel_area
GROUP BY hapar_id,
         YEAR,
         sum,
         saf.land_parcel_area

--TODO number of landuses per fid 
--TODO types of land use categories 
--TODO number seasonal renters 

Specific problems: 
Remaining problem hapars: 999, 1442, 1970, 2597, 40016
-- good example to catch PGRS - RGR subdivision by user 369777, 1144

-- problems joins 40016, 401109 (two businesses claiming same piece)
-- check hapars 1144,1146,1725,1728
212811 40 ha of building
1923 is good example of three renters who are leasing out a portion of their leased land in same year



--! Look at these Doug
SELECT * 
FROM temp_seasonal 
WHERE hapar_id = 85859 -- so many claims? so many businesses? also these: 83863, 242798
--! look at spatial


--! should I match non_saf owners where other land_use exists for that year? 224600, 212811, 178656, 229246, 230767


--* SPATIAL MATCHES 

-- count of businesses per hapar_id (2016)
SELECT hapar_id,
       count(distinct businesses)
FROM
    (SELECT hapar_id,
            owner_habus_id AS businesses
     FROM ladss.saf_iacs_2016_2017_2018
     WHERE owner_habus_id IS NOT NULL
         AND year = 2016
     UNION SELECT hapar_id,
                  user_habus_id AS businesses
     FROM ladss.saf_iacs_2016_2017_2018
     WHERE user_habus_id IS NOT NULL
         AND year = 2016) foo
GROUP BY hapar_id

-- count of landused per hapar_id (2016)
SELECT hapar_id,
       count(distinct lu) INTO TABLE lu_per_hapar_2016
FROM
    (SELECT hapar_id,
            owner_land_use AS lu
     FROM ladss.saf_iacs_2016_2017_2018
     WHERE owner_land_use IS NOT NULL
         AND year = 2016
     UNION SELECT hapar_id,
                  user_land_use AS lu
     FROM ladss.saf_iacs_2016_2017_2018
     WHERE user_land_use IS NOT NULL
         AND year = 2016) foo
GROUP BY hapar_id

-- count of landuses per field 



--TODO      COMMONS --------------------------------------------------------
--TODO          hapar_id = 96993 has cg_hahol_id <> hahol_id (2016)
-- finds difference between bps_eligible_area and total payment_regions
SELECT cg_hahol_id,
       hapar_id,
       YEAR,
       digitised_area,
       excluded_land_area,
       bps_eligible_area,
       region_total,
       bps_eligible_area - region_total AS diff
FROM
    (SELECT cg_hahol_id,
            hapar_id,
            YEAR,
            digitised_area,
            bps_eligible_area,
            excluded_land_area,
            payment_region_1 + payment_region_2 + payment_region_3 AS region_total
     FROM rpid.common_grazing_lpid_detail_deliv20190911) foo
WHERE bps_eligible_area <> region_total

-- Groups parcels by cg_hahol_id
--CREATE TABLE ladss.snapshot_2017_by_cg_holding AS
SELECT hahol_id,
       geom,
       ST_AREA(geom)
FROM
    (SELECT hahol_id,
            ST_Collect(geom) AS geom
     FROM ladss.snapshot_2017
     GROUP BY hahol_id) bar


-- finds differences between areas
SELECT hapar_id,
       cg_hahol_id,
       hahol_id,
       start_date,
       ROUND(CAST(ST_Area(geom) * 0.0001 AS NUMERIC), 2) AS calced_has,
       digitised_area,
       ABS(ROUND(CAST(ST_Area(geom) * 0.0001 AS NUMERIC), 2) - digitised_area) AS digi_diff,
       payment_region_1 + payment_region_2 + payment_region_3 AS sum_pay_regs,
       bps_eligible_area,
       excluded_land_area
FROM rpid.snapshot_2017_0417_peudonymised
JOIN rpid.common_grazing_lpid_detail_deliv20190911 USING (hapar_id)
WHERE year = 2016
    AND ABS(ROUND(CAST(ST_Area(geom) * 0.0001 AS NUMERIC), 2) - digitised_area) <> 0
