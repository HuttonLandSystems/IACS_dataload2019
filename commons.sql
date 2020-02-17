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

-- group by hahol_id and sum geom from snapshot // gets digi_area from grouped 
DROP TABLE IF EXISTS snapshot17;
SELECT hahol_id,
       ST_Area(ST_COLLECT(geom)) * 0.0001 AS digi_area INTO TEMP TABLE snapshot17
FROM rpid.snapshot_2017_0417_peudonymised
WHERE hahol_id IN (SELECT cg_hahol_id FROM lpid)
GROUP BY hahol_id;

-- Create combined tables and add hutton fields 
DROP TABLE IF EXISTS commons;
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
       CONCAT(lpid.id, ', ', share.id) AS id INTO TEMP TABLE commons
FROM lpid
JOIN share USING (year,
                  cg_hahol_id)
JOIN snapshot17 ON cg_hahol_id = hahol_id
WHERE year = 2016;
    
ALTER TABLE commons ADD COLUMN change_note VARCHAR; 
ALTER TABLE commons ADD COLUMN error_log VARCHAR; 
-- 10,293 rows

-- find mistakes and fix errors
UPDATE commons c
SET commons_area = c.digi_area,
    change_note = CONCAT('changed commons_area by ', CAST(abs(c.digi_area - c.lpid_area) AS VARCHAR), 'ha based on digi_area; ')
FROM
    (SELECT cg_hahol_id,
            digi_area,
            lpid_area,
            abs(digi_area - lpid_area),
            year
     FROM commons) sub
WHERE c.digi_area <> c.lpid_area
    AND abs(c.digi_area - c.lpid_area) <> 0
    AND c.cg_hahol_id = sub.cg_hahol_id
    AND c.year = sub.year
    AND c.lpid_area = sub.lpid_area; -- 5,887 rows

ALTER TABLE commons DROP COLUMN digi_area; 
ALTER TABLE commons DROP COLUMN lpid_area;

--convert blanks to rgr
UPDATE commons c 
SET land_use = 'RGR',
    change_note = CONCAT(change_note, 'changed blank land_use to RGR; ')
WHERE land_use = ''; -- 120 rows

-- make lpid_bps_eligible_area match parcel area where eligible area is larger 
UPDATE commons c 
SET lpid_bps_eligible_area = commons_area, 
    change_note = CONCAT(change_note, 'changed lpid_bps_eligible_area to match commons where eligible area larger by ', CAST(ABS(commons_area - lpid_bps_eligible_area) AS VARCHAR), 'ha ; ')
WHERE commons_area < lpid_bps_eligible_area; -- 62 rows


--TODO how to fix difference between eligible_area and claimed_area?
SELECT cg_hahol_id,
       lpid_bps_eligible_area,
       sum,
       abs(lpid_bps_eligible_area - sum) AS diff
FROM
    (SELECT cg_hahol_id,
            lpid_bps_eligible_area,
            sum(bps_claimed_area)
     FROM commons
     GROUP BY cg_hahol_id,
              lpid_bps_eligible_area) foo
WHERE sum > lpid_bps_eligible_area
ORDER BY abs(lpid_bps_eligible_area - sum)


--TODO double share_hahol_id per cg_hahol_id?


--TODO  convert deleted landuse to excl (or rather just include it in the excl table)


-- TODO   why is share_area = 0? 248 rows
--TODO    why does bps_claimed_area = lfass_claimed_area?  9819/10293 = 95%!!!

--TODO    finds when bps_claimed_area <> lfass_claimed_area and neither of them = 0
--TODO is this a problem?
SELECT * 
FROM commons 
WHERE bps_claimed_area <> lfass_claimed_area AND (bps_claimed_area <> 0 AND lfass_claimed_area <> 0)

