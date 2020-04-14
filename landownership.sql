DROP TABLE IF EXISTS ladss.saf_iacs_landownership_2018;


SELECT hapar_id,
       owner_habus_id,
       user_habus_id,
       CASE
           WHEN llo @> '{Y}' THEN 'Y'
           ELSE 'N'
       END AS llo,
       ARRAY_LENGTH(owner_habus_id, 1) AS owners_per_fid,
       ARRAY_LENGTH(user_habus_id, 1) AS users_per_fid,
       CASE
           WHEN owner_habus_id IS NULL THEN ARRAY_LENGTH(user_habus_id, 1)
           WHEN user_habus_id IS NULL THEN ARRAY_LENGTH(owner_habus_id, 1)
           ELSE ARRAY_LENGTH(owner_habus_id, 1) + ARRAY_LENGTH(user_habus_id, 1)
       END AS brns_per_fid,
       owner_bps_claimed_area,
       user_bps_claimed_area INTO ladss.saf_iacs_landownership_2018
FROM
    (SELECT hapar_id,
            CASE
                WHEN owner_habus_id = '{}' THEN NULL
                ELSE owner_habus_id
            END AS owner_habus_id,
            CASE
                WHEN user_habus_id = '{}' THEN NULL
                ELSE user_habus_id
            END AS user_habus_id,
            ARRAY_CAT(owner_llo, user_llo) AS llo,
            CASE
                WHEN owner_bps_claimed_area = 0 THEN NULL
                ELSE owner_bps_claimed_area
            END AS owner_bps_claimed_area,
            CASE
                WHEN user_bps_claimed_area = 0 THEN NULL
                ELSE user_bps_claimed_area
            END AS user_bps_claimed_area
     FROM
         (SELECT hapar_id,
                 ARRAY_REMOVE(owner_habus_id, NULL) AS owner_habus_id,
                 ARRAY_REMOVE(user_habus_id, NULL) AS user_habus_id,
                 ARRAY_REMOVE(owner_llo, NULL) AS owner_llo,
                 ARRAY_REMOVE(user_llo, NULL) AS user_llo,
                 owner_bps_claimed_area,
                 user_bps_claimed_area
          FROM
              (SELECT hapar_id,
                      ARRAY_AGG(DISTINCT owner_habus_id
                                ORDER BY owner_habus_id) AS owner_habus_id,
                      ARRAY_AGG(DISTINCT user_habus_id
                                ORDER BY user_habus_id) AS user_habus_id,
                      ARRAY_AGG(DISTINCT owner_llo
                                ORDER BY owner_llo) AS owner_llo,
                      ARRAY_AGG(DISTINCT user_llo
                                ORDER BY user_llo) AS user_llo,
                      SUM(owner_bps_claimed_area) AS owner_bps_claimed_area,
                      SUM(user_bps_claimed_area) AS user_bps_claimed_area
               FROM
                   (SELECT hapar_id_v2 AS hapar_id,
                           owner_habus_id,
                           user_habus_id,
                           SUM(owner_bps_claimed_area) AS owner_bps_claimed_area,
                           SUM(user_bps_claimed_area) AS user_bps_claimed_area,
                           owner_land_leased_out AS owner_llo,
                           user_land_leased_out AS user_llo
                    FROM ladss.saf_iacs_2018_processed
                    WHERE hapar_id_v2 NOT IN
                            (SELECT hapar_id_v2
                             FROM ladss.saf_iacs_2018_processed
                             WHERE error_log LIKE '%commons%')
                    GROUP BY hapar_id_v2,
                             owner_habus_id,
                             user_habus_id,
                             owner_land_leased_out,
                             user_land_leased_out) foo
               GROUP BY hapar_id) bar) foobar) foobar2;

-- Spatial join in cte 
DROP TABLE IF EXISTS ladss.aw_and_iacs_landownership_2018 ;
WITH cte AS
    (SELECT hapar_id_v2 AS hapar_id,
            property,
            ST_AREA(aw.geom) * 0.0001 AS aw_area_ha,
            area_ha AS iacs_area_ha,
            ST_AREA(ST_INTERSECTION(aw.geom, iacs.geom)) * 0.0001 AS area_of_intersection
     FROM ladss.saf_iacs_2018_processed_fields iacs
     JOIN
         (SELECT property,
                 ST_BUFFER(ST_COLLECT(st_collectionextract(geom, 3)), 0.0) AS geom
          FROM ladss.aw_landownership_2015
          GROUP BY property) aw ON ST_INTERSECTS(aw.geom, iacs.geom))
SELECT property,
       aw_area_ha,
       iacs_area_ha,
       area_of_intersection,
       hapar_id,
       owner_habus_id,
       user_habus_id,
       llo,
       owners_per_fid,
       users_per_fid,
       brns_per_fid,
       owner_bps_claimed_area, 
       user_bps_claimed_area INTO ladss.aw_and_iacs_landownership_2018
FROM cte
JOIN ladss.saf_iacs_landownership_2018 USING (hapar_id); 

-- aggregates on property // replicates Paola's table sent 10 April 2020 Friday
SELECT property,
       aw_area_ha,
       sum_iacs_area,
       sum_intersection_area,
       percent_intersection,
       llo,
       (llo_n/llo_length) * 100.0 AS percent_llo_n,
       (llo_y/llo_length) * 100.0 AS percent_llo_yes,
       distinct_owners,
       distinct_users,
       owner_bps_claimed_area,
       user_bps_claimed_area
FROM
    (SELECT property,
            aw_area_ha,
            sum_iacs_area,
            sum_intersection_area,
            percent_intersection,
            llo,
            CAST(llo_y AS FLOAT) AS llo_y,
            CAST(llo_n AS FLOAT) AS llo_n,
            ARRAY_LENGTH(llo, 1) AS llo_length,
            ARRAY_LENGTH(owner_habus_id, 1) AS distinct_owners,
            ARRAY_LENGTH(user_habus_id, 1) AS distinct_users,
            owner_bps_claimed_area,
            user_bps_claimed_area
     FROM
         (SELECT property,
                 aw_area_ha,
                 sum_iacs_area,
                 sum_intersection_area,
                 percent_intersection,
                 llo,
                 llo_y,
                 llo_n,
                 ARRAY_LENGTH(llo, 1) AS llo_length,
                 ANYARRAY_UNIQ(owner_habus_id) AS owner_habus_id,
                 ANYARRAY_UNIQ(user_habus_id) AS user_habus_id,
                 owner_bps_claimed_area,
                 user_bps_claimed_area
          FROM
              (SELECT property,
                      aw_area_ha,
                      sum(iacs_area_ha) AS sum_iacs_area,
                      sum(area_of_intersection) AS sum_intersection_area,
                      (sum(area_of_intersection)/aw_area_ha) * 100 AS percent_intersection,
                      ARRAY_AGG(llo
                                ORDER BY llo) AS llo,
                      sum(CASE
                              WHEN llo='Y' then 1
                          END) AS llo_y,
                      SUM(CASE
                              WHEN llo='N' then 1
                          END) AS llo_n,
                      ANYARRAY_AGG(owner_habus_id) AS owner_habus_id,
                      ANYARRAY_AGG(user_habus_id) AS user_habus_id,
                      SUM(owner_bps_claimed_area) AS owner_bps_claimed_area,
                      SUM(user_bps_claimed_area) AS user_bps_claimed_area
               FROM ladss.aw_and_iacs_landownership_2018
               GROUP BY property,
                        aw_area_ha) goo) foo) foobar;