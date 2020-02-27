-- create table to rank land_uses based on intensity
DROP TABLE IF EXISTS LU_rank;
CREATE TEMP TABLE lu_rank
(land_use VARCHAR(300), rank INTEGER);

INSERT INTO lu_rank (land_use , rank)
VALUES
    ('ALF', 1),('AMCP', 1),('ARTC', 1),('ASPG', 1),('ASSF', 1),
    ('BFLO', 1),('BFT', 1),('BKB', 1),('BLB', 1),('BLR-OPEN', 1),
    ('BLR-POLY', 1),('BLU-OPEN', 1),('BPP', 1),('BRT', 1),('BSP', 1),
    ('BW', 1),('CABB', 1),('CALA', 1),('CANS', 1),('CARR', 1),
    ('CAUL', 1),('CEL', 1),('CHIC', 1),('CHP', 1),('CLO', 1),
    ('CRB', 1),('ENG-B', 1),('GAR', 1),('GSB', 1),('HS', 1),
    ('LEEK', 1),('LEN', 1),('LETT', 1),('LGB', 1),('LIN', 1),
    ('MAIZ', 1),('MBSF', 1),('MSC', 1),('MU', 1),('NU-FS', 1),
    ('NU-OT', 1),('NU-SH', 1),('OCS-B', 1),('OCS-K', 1),('ONI', 1),
    ('ONU', 1),('OSFRT', 1),('OVEG', 1),('PAR', 1),('PUM', 1),
    ('RASP-OPEN', 1),('RASP-POLY', 1),('RAST', 1),('RHB', 1),('RRC', 1),
    ('SB', 1),('SBEAN', 1),('SFB', 1),('SL', 1),('SO', 1),('SOSR', 1),
    ('SPEAS', 1),('SPOT', 1),('SPP', 1),('SRYE', 1),('SSF', 1),
    ('STRB-OPEN', 1),('STRB-POLY', 1),('STRIT', 1),('STS', 1),('SUN', 1),
    ('SW', 1),('SWS', 1),('TFRT', 1),('TSF', 1),('WB', 1),
    ('WBEAN', 1),('WBS', 1),('WFB', 1),('WO', 1),('WOSR', 1),('WPEAS', 1),
    ('WPOT', 1),('WPP', 1),('WRYE', 1),('WTRIT', 1),('WW', 1),('EX-SS', 2),
    ('FALW', 2),('FALW-5', 2),('GCM', 2),('NETR-A', 2),('NETR-NA', 2),('PHA', 2),
    ('SRC', 2),('UCAA', 2),('WDG', 2),('WFM', 2),('TGRS', 3),('TGRS1', 3),('TGRS2', 3),
    ('TGRS3', 3),('TGRS4', 3),('TGRS5', 3),('VET', 3),('PC', 4),('PGRS', 4),
    ('RGR', 5),('BRA', 99),('BUI', 99),('EXCL', 99),('FSE', 99),('GOR', 99),
    ('MAR', 99),('RASP-GLS', 99),('ROAD', 99),('ROK', 99),('SCB', 99),
    ('SCE', 99),('STRB-GLS', 99),('TOM-GLS', 99),('TREES', 99),
    ('TURF', 99),('WAT', 99);

WITH cte AS
    (SELECT hapar_id,
            land_use,
            land_parcel_area,
            CASE
                WHEN owner_bps_claimed_area + user_bps_claimed_area <> 0 THEN owner_bps_claimed_area + user_bps_claimed_area
                ELSE owner_land_use_area + user_land_use_area
            END AS used_area
     FROM
        (SELECT hapar_id_v2 AS hapar_id,
            CASE
                WHEN user_land_use IS NULL
                        OR user_land_use = 'NON_SAF' THEN owner_land_use
                ELSE user_land_use
            END AS land_use,
            land_parcel_area,
            CASE
                WHEN owner_bps_claimed_area IS NULL THEN 0
                ELSE owner_bps_claimed_area
            END AS owner_bps_claimed_area,
            CASE
                WHEN user_bps_claimed_area IS NULL THEN 0
                ELSE user_bps_claimed_area
            END AS user_bps_claimed_area,
            CASE
                WHEN owner_land_use_area IS NULL THEN 0
                ELSE owner_land_use_area
            END AS owner_land_use_area,
            CASE
                WHEN user_land_use_area IS NULL THEN 0
                ELSE user_land_use_area
            END AS user_land_use_area
        FROM ladss.saf_iacs_2018_processed
        WHERE hapar_id NOT IN
                (SELECT DISTINCT hapar_id
                FROM ladss.saf_iacs_2016_2017_2018
                WHERE error_log LIKE '%common%')) foo)
SELECT hapar_id, 
        land_use, 
        used_area AS largest_used_area INTO ladss.saf_IACS_2018_largest_use_per_fid
FROM (
SELECT hapar_id,
       land_use,
       used_area,
       ROW_NUMBER() OVER (PARTITION BY hapar_id
                          ORDER BY used_area DESC, rank) AS rn
FROM
    (SELECT hapar_id,
            land_use,
            SUM(used_area) AS used_area
     FROM cte
     GROUP BY hapar_id,
              land_use) bar
JOIN lu_rank USING (land_use)) foobar 
WHERE rn = 1;

-- COMMONS
----------------------------------------------------------------------------------
SELECT cg_hahol_id,
       land_use,
       used_area INTO ladss.saf_commons_2018_largest_use_per_fid
FROM
    ( SELECT cg_hahol_id,
             land_use,
             sum_bps AS used_area,
             ROW_NUMBER() OVER (PARTITION BY cg_hahol_id
                                ORDER BY sum_bps DESC, rank) AS rn
     FROM
         (SELECT cg_hahol_id,
                 land_use,
                 sum(bps_claimed_area) AS sum_bps
          FROM ladss.saf_commons_2016_2017_2018
          WHERE YEAR = 2018
          GROUP BY cg_hahol_id,
                   land_use) foo
     JOIN lu_rank USING (land_use)) bar
WHERE rn = 1;
              