-- Create prelim temp tables with IDs
DROP TABLE IF EXISTS share;
SELECT * INTO TEMP TABLE share 
FROM rpid.saf_common_grazing_share_usage_deliv20190911;

ALTER TABLE share ADD COLUMN id SERIAL;

ALTER TABLE share  
ALTER COLUMN id TYPE VARCHAR;

UPDATE share 
SET id = CONCAT('S', id);
--- 43,566 rows

-- group by cg_hahol_id, year and sum everything else from lpid detail table
DROP TABLE IF EXISTS lpid; 
SELECT cg_hahol_id,
       YEAR,
       sum(digitised_area) AS lpid_area,
       sum(bps_eligible_area) AS lpid_bps_eligible_area,
       sum(excluded_land_area) AS lpid_excluded_land_area INTO TEMP TABLE lpid
FROM rpid.common_grazing_lpid_detail_deliv20190911
GROUP BY cg_hahol_id,
         year;

ALTER TABLE lpid ADD COLUMN id SERIAL; 

ALTER TABLE lpid 
ALTER COLUMN id TYPE VARCHAR;

UPDATE lpid
SET id = CONCAT('L', id);
--- 3,675 rows

-- 2016 group by hahol_id and sum geom from snapshot // gets digi_area from grouped 
DROP TABLE IF EXISTS snapshot17;
SELECT hahol_id,
       ST_Area(ST_COLLECT(geom)) * 0.0001 AS digi_area INTO TEMP TABLE snapshot17
FROM rpid.snapshot_2017_0417_peudonymised
WHERE hahol_id IN
        (SELECT cg_hahol_id
         FROM lpid
         WHERE year = 2016)
GROUP BY hahol_id; -- 918 rows / distinct cg_hahol_id -- ALL MATCH 

-- 2017 group by hahol_id and sum geom from snapshot // gets digi_area from grouped 
DROP TABLE IF EXISTS snapshot18;
SELECT farm_code_hahol_id AS hahol_id,
       ST_Area(ST_COLLECT(wkb_geometry)) * 0.0001 AS digi_area INTO TEMP TABLE snapshot18
FROM rpid.lpis_land_parcels_2018_jhi_deliv20190911
WHERE farm_code_hahol_id IN
        (SELECT cg_hahol_id
         FROM lpid
         WHERE year = 2017)
GROUP BY farm_code_hahol_id; -- 919 rows / distinct cg_hahol_id -- ALL MATCH 

-- 2018 group by hahol_id and sum geom from snapshot // gets digi_area from grouped
DROP TABLE IF EXISTS snapshot19;
SELECT farm_code_hahol_id AS hahol_id,
       ST_Area(ST_COLLECT(wkb_geometry)) * 0.0001 AS digi_area INTO TEMP TABLE snapshot19
FROM rpid.lpis_land_parcels_2019_jhi_deliv20190911
WHERE farm_code_hahol_id IN
        (SELECT cg_hahol_id
         FROM lpid
         WHERE year = 2018)
GROUP BY farm_code_hahol_id; -- 919 rows / distinct cg_hahol_id -- ALL MATCH 

-- 2016 Create combined table and add hutton fields 
DROP TABLE IF EXISTS commons16;
SELECT cg_hahol_id,
       mlc_hahol_id,
       share_hahol_id,
       habus_id,
       ROUND(CAST(digi_area AS NUMERIC), 2) AS commons_area,
       ROUND(CAST(digi_area AS NUMERIC), 2) AS digi_area,
       lpid_area,
       area_amt AS share_area,
       lpid_bps_eligible_area,
       share_bps_eligible_area AS share_bps_eligible_area,
       bps_claimed_area,
       lfass_claimed_area,
       lpid_excluded_land_area,
       activity_type_desc AS land_activity,
       name AS land_use,
       year,
       CONCAT(lpid.id, ', ', share.id) AS id INTO TEMP TABLE commons16
FROM lpid
JOIN share USING (year,
                  cg_hahol_id)
JOIN snapshot17 ON cg_hahol_id = hahol_id
WHERE year = 2016; -- 10,293 rows
    
ALTER TABLE commons16 ADD COLUMN change_note VARCHAR; 
ALTER TABLE commons16 ADD COLUMN error_log VARCHAR; 

-- 2017 Create combind table and add hutton fields
DROP TABLE IF EXISTS commons17;
SELECT cg_hahol_id,
       mlc_hahol_id,
       share_hahol_id,
       habus_id,
       ROUND(CAST(digi_area AS NUMERIC), 2) AS commons_area,
       ROUND(CAST(digi_area AS NUMERIC), 2) AS digi_area,
       lpid_area,
       area_amt AS share_area,
       lpid_bps_eligible_area,
       share_bps_eligible_area AS share_bps_eligible_area,
       bps_claimed_area,
       lfass_claimed_area,
       lpid_excluded_land_area,
       activity_type_desc AS land_activity,
       name AS land_use,
       year,
       CONCAT(lpid.id, ', ', share.id) AS id INTO TEMP TABLE commons17
FROM lpid
JOIN share USING (year,
                  cg_hahol_id)
JOIN snapshot18 ON cg_hahol_id = hahol_id
WHERE year = 2017; -- 10,183 rows
    
ALTER TABLE commons17 ADD COLUMN change_note VARCHAR; 
ALTER TABLE commons17 ADD COLUMN error_log VARCHAR; 

-- 2018 Create combined table and add hutton fields 
DROP TABLE IF EXISTS commons18;
SELECT cg_hahol_id,
       mlc_hahol_id,
       share_hahol_id,
       habus_id,
       ROUND(CAST(digi_area AS NUMERIC), 2) AS commons_area,
       ROUND(CAST(digi_area AS NUMERIC), 2) AS digi_area,
       lpid_area,
       area_amt AS share_area,
       lpid_bps_eligible_area,
       share_bps_eligible_area AS share_bps_eligible_area,
       bps_claimed_area,
       lfass_claimed_area,
       lpid_excluded_land_area,
       activity_type_desc AS land_activity,
       name AS land_use,
       year,
       CONCAT(lpid.id, ', ', share.id) AS id INTO TEMP TABLE commons18
FROM lpid
JOIN share USING (year,
                  cg_hahol_id)
JOIN snapshot19 ON cg_hahol_id = hahol_id
WHERE year = 2018; -- 10,164 rows
    
ALTER TABLE commons18 ADD COLUMN change_note VARCHAR; 
ALTER TABLE commons18 ADD COLUMN error_log VARCHAR; 

-- 2016 find differences in area fields commons_area, digi_area and lpid_area 
UPDATE commons16 c
SET commons_area = c.digi_area,
    change_note = CONCAT('changed commons_area by ', CAST(abs(c.digi_area - c.lpid_area) AS VARCHAR), 'ha based on digi_area; ')
FROM
    (SELECT cg_hahol_id,
            digi_area,
            lpid_area,
            abs(digi_area - lpid_area),
            year
     FROM commons16) sub
WHERE c.digi_area <> c.lpid_area
    AND abs(c.digi_area - c.lpid_area) <> 0
    AND c.cg_hahol_id = sub.cg_hahol_id
    AND c.year = sub.year
    AND c.lpid_area = sub.lpid_area; -- 5,887 rows

-- 2017 find differences in area fields commons_area, digi_area and lpid_area 
UPDATE commons17 c
SET commons_area = c.digi_area,
    change_note = CONCAT('changed commons_area by ', CAST(abs(c.digi_area - c.lpid_area) AS VARCHAR), 'ha based on digi_area; ')
FROM
    (SELECT cg_hahol_id,
            digi_area,
            lpid_area,
            abs(digi_area - lpid_area),
            year
     FROM commons17) sub
WHERE c.digi_area <> c.lpid_area
    AND abs(c.digi_area - c.lpid_area) <> 0
    AND c.cg_hahol_id = sub.cg_hahol_id
    AND c.year = sub.year
    AND c.lpid_area = sub.lpid_area; -- 4,601 rows 

-- 2018 find differences in area fields commons_area, digi_area and lpid_area 
UPDATE commons18 c
SET commons_area = c.digi_area,
    change_note = CONCAT('changed commons_area by ', CAST(abs(c.digi_area - c.lpid_area) AS VARCHAR), 'ha based on digi_area; ')
FROM
    (SELECT cg_hahol_id,
            digi_area,
            lpid_area,
            abs(digi_area - lpid_area),
            year
     FROM commons18) sub
WHERE c.digi_area <> c.lpid_area
    AND abs(c.digi_area - c.lpid_area) <> 0
    AND c.cg_hahol_id = sub.cg_hahol_id
    AND c.year = sub.year
    AND c.lpid_area = sub.lpid_area; -- 4,632 rows

-- clean up 
ALTER TABLE commons16 DROP COLUMN digi_area; 
ALTER TABLE commons16 DROP COLUMN lpid_area;
ALTER TABLE commons17 DROP COLUMN digi_area; 
ALTER TABLE commons17 DROP COLUMN lpid_area;
ALTER TABLE commons18 DROP COLUMN digi_area; 
ALTER TABLE commons18 DROP COLUMN lpid_area;
DROP TABLE IF EXISTS share;
DROP TABLE IF EXISTS lpid; 
DROP TABLE IF EXISTS snapshot17;

-- combine years
DROP TABLE IF EXISTS commons;
CREATE TEMP TABLE commons AS
    (SELECT *
     FROM commons16
     UNION SELECT *
     FROM commons17
     UNION SELECT *
     FROM commons18); -- 30,640 rows

-- delete 0 share_area
DELETE
FROM commons
WHERE share_area = 0
    AND bps_claimed_area = 0; -- 658 rows all have 0 bps_claimed_area

-- make lpid_bps_eligible_area match parcel area where eligible area is larger 
UPDATE commons c 
SET lpid_bps_eligible_area = commons_area, 
    change_note = CONCAT(change_note, 'changed lpid_bps_eligible_area to match commons where eligible area larger by ', CAST(ABS(commons_area - lpid_bps_eligible_area) AS VARCHAR), 'ha ; ')
WHERE commons_area < lpid_bps_eligible_area; -- 364 rows

--TODO how to fix difference between eligible_area and claimed_area?
SELECT cg_hahol_id,
       lpid_bps_eligible_area,
       share_claimed_area,
       year,
       abs(lpid_bps_eligible_area - share_claimed_area) AS diff
FROM
    (SELECT cg_hahol_id,
            lpid_bps_eligible_area,
            year,
            sum(bps_claimed_area) AS share_claimed_area
     FROM commons
     GROUP BY cg_hahol_id,
              lpid_bps_eligible_area, 
              year) foo
WHERE share_claimed_area > lpid_bps_eligible_area
ORDER BY abs(lpid_bps_eligible_area - share_claimed_area);

--TODO double share_hahol_id per cg_hahol_id?
SELECT *
FROM
    (SELECT cg_hahol_id,
            share_hahol_id,
            year,
            COUNT(DISTINCT land_use) AS no_lUs
     FROM
         (SELECT cg_hahol_id,
                 share_hahol_id,
                 year,
                 count(*)
          FROM commons
          GROUP BY cg_hahol_id,
                   share_hahol_id,
                   year) foo
     JOIN commons c USING (cg_hahol_id,
                           share_hahol_id,
                           year)
     WHERE count > 1
     GROUP BY cg_hahol_id,
              share_hahol_id,
              year) bar
JOIN commons c USING (cg_hahol_id,
                      share_hahol_id,
                      year)
WHERE no_lUs = 1
ORDER BY cg_hahol_id,
         share_hahol_id

--TODO combine claims with same cg_hahol_id, mlc_hahol_id, share_hahol_id, habus_id, share_bps_eligible_area, land_use
-- sum(share_area) and sum(bps_claimed_area) and sum(lfass_claimed_area) in the update
WITH mult_claims AS
    (SELECT *
     FROM
         (SELECT cg_hahol_id,
                 mlc_hahol_id,
                 share_hahol_id,
                 habus_id,
                 share_bps_eligible_area,
                 land_use,
                 COUNT(*)
          FROM commons
          GROUP BY cg_hahol_id,
                   mlc_hahol_id,
                   share_hahol_id,
                   habus_id,
                   share_bps_eligible_area,
                   land_use) foo
     JOIN commons c USING (cg_hahol_id,
                           mlc_hahol_id,
                           share_hahol_id,
                           habus_id)
     WHERE count > 1)
SELECT cg_hahol_id,
       mlc_hahol_id,
       share_hahol_id,
       habus_id,
       commons_area,
       sum(share_area) AS share_area,
       lpid_bps_eligible_area,
       share_bps_eligible_area,
       sum(bps_claimed_area) AS bps_claimed_area,
       sum(lfass_claimed_area) AS lfass_claimed_area,
       lpid_excluded_land_area,
       'Undertaking Production Activities' AS land_activity,
       land_use,
       year,
       STRING_AGG(id, ', '),
       change_note,
       error_log
FROM mult_claims
GROUP BY cg_hahol_id,
         mlc_hahol_id,
         share_hahol_id,
         habus_id,
         commons_area,
         lpid_bps_eligible_area,
         share_bps_eligible_area,
         lpid_excluded_land_area,
         'Undertaking Production Activities' AS land_activity,
         land_use,
         year,
         change_note,
         error_log



--TODO  convert deleted landuse to excl (or rather just include it in the excl table)



--TODO    why does bps_claimed_area = lfass_claimed_area?  9819/10293 = 95%!!!

--TODO    finds when bps_claimed_area <> lfass_claimed_area and neither of them = 0
--TODO is this a problem?
SELECT * 
FROM commons 
WHERE bps_claimed_area <> lfass_claimed_area AND (bps_claimed_area <> 0 AND lfass_claimed_area <> 0)

