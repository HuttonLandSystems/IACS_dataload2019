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
     FROM rpid.lpis_land_parcels_2018_jhi_deliv20190911
     JOIN final AS l USING (hapar_id)
     WHERE l.YEAR = 2017
     GROUP BY hapar_id, digi_area) foo

-- 5,073,818/6,479,879 hectares = 78.3% of 2019 parcels (area)
SELECT SUM(digi_area) FROM
    (SELECT hapar_id, digi_area
     FROM rpid.lpis_land_parcels_2019_jhi_deliv20190911
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
-- 444,518‬/525,621 = 84.6% of 2018 parcels have at least one claim associated 
-- 440,026‬/526,899 = 83.5% of 2019 parcels have at least one claim associated 
SELECT hapar_id
FROM rpid.lpis_land_parcels_2019_jhi_deliv20190911
EXCEPT
SELECT hapar_id
FROM final AS l
WHERE l.YEAR = 2018

-- ORIGINAL DATA      
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

-- 444,518‬/449,556‬ = 98.9%
-- 440,026/451,774 = 97.4%

--TODO how many time does digi_area match land_parcel_area within 0.1 and 0.01 threshold 
--TODO count of user businesses map
--TODO underclaims / overclaims
--TODO number of landuses per fid 
--TODO types of land use categoriezed 
--TODO number seasonal renters 


