DROP TEMP TABLE IF EXISTS LU_rank;
CREATE TEMP TABLE LU_rank(
   land_use VARCHAR(7) 
  ,rank     INTEGER  
);
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

DROP TABLE IF EXISTS ladss.saf_iacs_largest_use_per_fid;

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
               HAVING sum(owner_bps_claimed_area) + sum(user_bps_claimed_area) IS NOT NULL
               AND sum(owner_bps_claimed_area) + sum(user_bps_claimed_area) <> 0
               AND land_use <> 'EXCL') foorbar) foobar2
     GROUP BY hapar_id,
              land_use)
SELECT hapar_id,
       land_use,
       max_claimed INTO ladss.saf_iacs_largest_use_per_fid
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

