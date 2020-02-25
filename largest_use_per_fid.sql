DROP TABLE IF EXISTS LU_rank;
CREATE TEMP TABLE LU_rank( land_use VARCHAR(7),
                                    rank INTEGER);
INSERT INTO LU_rank(land_use,rank) VALUES ('SB',1);
INSERT INTO LU_rank(land_use,rank) VALUES ('SO',1);
INSERT INTO LU_rank(land_use,rank) VALUES ('SPOT',1);
INSERT INTO LU_rank(land_use,rank) VALUES ('WBS',1);
INSERT INTO LU_rank(land_use,rank) VALUES ('WOSR',1);
INSERT INTO LU_rank(land_use,rank) VALUES ('WW',1);
INSERT INTO LU_rank(land_use,rank) VALUES ('FALW',2);
INSERT INTO LU_rank(land_use,rank) VALUES ('NETR_NA',2);
INSERT INTO LU_rank(land_use,rank) VALUES ('PC',2);
INSERT INTO LU_rank(land_use,rank) VALUES ('SRC',2);
INSERT INTO LU_rank(land_use,rank) VALUES ('UCAA',2);
INSERT INTO LU_rank(land_use,rank) VALUES ('TGRS',3);
INSERT INTO LU_rank(land_use,rank) VALUES ('PGRS',4);
INSERT INTO LU_rank(land_use,rank) VALUES ('WDG',5);
INSERT INTO LU_rank(land_use,rank) VALUES ('RGR',6);

DROP TABLE IF EXISTS ladss.saf_iacs_2018_largest_claim_per_fid;
WITH cte AS
    (SELECT hapar_id,
            land_use,
            sum(claimed_area) AS claimed_area
     FROM
         (SELECT hapar_id,
                 CASE
                     WHEN SUBSTRING(land_use
                                    FROM 1
                                    FOR 4) = 'TGRS' THEN 'TGRS'
                     ELSE land_use
                 END AS land_use,
                 claimed_area
          FROM
              (SELECT hapar_id,
                      land_use,
                      sum(owner_bps_claimed_area) + sum(user_bps_claimed_area) AS claimed_area
               FROM
                   (SELECT hapar_id_v2 AS hapar_id,
                           CASE
                               WHEN user_land_use IS NULL
                                    OR user_land_use = 'NON_SAF' THEN owner_land_use
                               ELSE user_land_use
                           END AS land_use,
                           CASE
                               WHEN owner_bps_claimed_area IS NULL THEN 0
                               ELSE owner_bps_claimed_area
                           END AS owner_bps_claimed_area,
                           CASE
                               WHEN user_bps_claimed_area IS NULL THEN 0
                               ELSE user_bps_claimed_area
                           END AS user_bps_claimed_area
                    FROM ladss.saf_iacs_2018_processed) foo
               GROUP BY hapar_id,
                        land_use
               HAVING sum(owner_bps_claimed_area) + sum(user_bps_claimed_area) <> 0
               AND land_use <> 'EXCL') foorbar) foobar2
     GROUP BY hapar_id,
              land_use)
SELECT hapar_id,
       land_use,
       max_claimed INTO ladss.saf_iacs_2018_largest_claim_per_fid
FROM
    (SELECT cte.hapar_id,
            land_use,
            max_claimed,
            ROW_NUMBER() OVER (PARTITION BY cte.hapar_id,
                                            land_use
                               ORDER BY rank)
     FROM
         (SELECT hapar_id,
                 max(claimed_area) AS max_claimed
          FROM cte
          GROUP BY hapar_id
          HAVING MAX(claimed_area) <> 0) foo
     JOIN cte ON cte.hapar_id = foo.hapar_id
     AND max_claimed = claimed_area
     JOIN lu_rank USING (land_use)) foobar
WHERE ROW_NUMBER = 1;

WITH cte AS
    (SELECT hapar_id,
            land_use,
            land_parcel_area,
            CASE
                WHEN bps_claimed_area <> 0 THEN bps_claimed_area
                ELSE land_use_area
            END AS used_area
     FROM
         (SELECT hapar_id,
                 land_use,
                 land_parcel_area,
                 owner_bps_claimed_area + user_bps_claimed_area AS bps_claimed_area,
                 owner_land_use_area + user_land_use_area AS land_use_area
          FROM
              (SELECT hapar_id,
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
                       (SELECT hapar_id
                        FROM ladss.saf_iacs_2018_largest_claim_per_fid)
                   AND (owner_land_use NOT IN
                            (SELECT land_use
                             FROM excl)
                        OR user_land_use NOT IN
                            (SELECT land_use
                             FROM excl))) foo) bar
     ORDER BY hapar_id)
SELECT hapar_id, 
        land_use, 
        used_area INTO ladss.saf_iacs_2018_largest_use_per_fid_exclusive
        FROM (     
SELECT hapar_id,
       land_use,
       sum(used_area) AS used_area,
       ROW_NUMBER() OVER (PARTITION BY hapar_id,
                                       sum(used_area)
                          ORDER BY sum(used_area) DESC, 
                                   land_use)
FROM cte
GROUP BY hapar_id,
         land_use) foobar 
         WHERE row_number = 1

-- COMMONS
----------------------------------------------------------------------------------
WITH cte AS
    (SELECT cg_hahol_id,
            land_use,
            sum(bps_claimed_area) AS sum_bps
     FROM ladss.saf_commons_2016_2017_2018
     WHERE YEAR = 2018
     GROUP BY cg_hahol_id,
              land_use)
SELECT cg_hahol_id,
       land_use,
       max_bps_claimed INTO ladss.saf_commons_2018_largest_claim_per_fid
FROM
    (SELECT foo.cg_hahol_id,
            land_use,
            max_bps_claimed,
            ROW_NUMBER() OVER (PARTITION BY foo.cg_hahol_id
                               ORDER BY land_use) AS rn
     FROM
         (SELECT cg_hahol_id,
                 MAX(sum_bps) AS max_bps_claimed
          FROM cte
          GROUP BY cg_hahol_id) foo
     JOIN cte AS self ON foo.cg_hahol_id = self.cg_hahol_id
     AND sum_bps = max_bps_claimed) foobar
WHERE rn = 1


