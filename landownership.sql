DROP TABLE IF EXISTS ladss.saf_iacs_landownership_2018;


SELECT hapar_id,
       owner_habus_id,
       user_habus_id,
       llo,
       owners_per_fid,
       users_per_fid,
       brns_per_fid,
       area_ha,
       geom INTO ladss.saf_iacs_landownership_2018
FROM
    (SELECT hapar_id,
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
            END AS brns_per_fid
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
                 ARRAY_CAT(owner_llo, user_llo) AS llo
          FROM
              (SELECT hapar_id,
                      ARRAY_REMOVE(owner_habus_id, NULL) AS owner_habus_id,
                      ARRAY_REMOVE(user_habus_id, NULL) AS user_habus_id,
                      ARRAY_REMOVE(owner_llo, NULL) AS owner_llo,
                      ARRAY_REMOVE(user_llo, NULL) AS user_llo
               FROM
                   (SELECT hapar_id,
                           ARRAY_AGG(DISTINCT owner_habus_id
                                     ORDER BY owner_habus_id) AS owner_habus_id,
                           ARRAY_AGG(DISTINCT user_habus_id
                                     ORDER BY user_habus_id) AS user_habus_id,
                           ARRAY_AGG(DISTINCT owner_llo
                                     ORDER BY owner_llo) AS owner_llo,
                           ARRAY_AGG(DISTINCT user_llo
                                     ORDER BY user_llo) AS user_llo
                    FROM
                        (SELECT hapar_id_v2 AS hapar_id,
                                owner_habus_id,
                                user_habus_id,
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
                    GROUP BY hapar_id) bar) foobar) foobar2
     ORDER BY hapar_id) foobar3
JOIN ladss.saf_iacs_2018_processed_fields ON hapar_id = hapar_id_v2;


DROP TABLE IF EXISTS ladss.aw_and_iacs_landownership_2018;


SELECT property,
       county,
       owner,
       address,
       postcode,
       aw_date_recorded,
       aw_area_ha,
       iacs_area_ha,
       ST_AREA(ST_INTERSECTION(aw_geom, iacs_geom)) * 0.0001 AS area_of_intersection,
       (ST_AREA(ST_INTERSECTION(aw_geom, iacs_geom)) * 0.0001) / iacs_area_ha AS percent_of_intersection,
       hapar_id,
       owner_habus_id,
       user_habus_id,
       llo,
       owners_per_fid,
       users_per_fid,
       brns_per_fid,
       aw_geom AS geom INTO ladss.aw_and_iacs_landownership_2018
FROM
    (SELECT property,
            county,
            owner,
            address,
            postcode,
            aw_date_recorded,
            aw_area_ha,
            iacs_area_ha,
            hapar_id,
            owner_habus_id,
            user_habus_id,
            llo,
            owners_per_fid,
            users_per_fid,
            brns_per_fid,
            iacs_geom,
            aw_geom
     FROM
         (SELECT code,
                 property,
                 county,
                 owner,
                 add_1 AS address,
                 postcode,
                 currency AS aw_date_recorded,
                 ST_AREA(geom) * 0.0001 AS aw_area_ha,
                 ST_BUFFER(geom, 0.0) AS aw_geom
          FROM ladss.aw_landownership_2015) aw
     JOIN
         (SELECT area_ha AS iacs_area_ha,
                 hapar_id,
                 owner_habus_id,
                 user_habus_id,
                 llo,
                 owners_per_fid,
                 users_per_fid,
                 brns_per_fid,
                 geom AS iacs_geom
          FROM ladss.saf_iacs_landownership_2018) iacs ON ST_INTERSECTS(aw_geom, iacs_geom)) goo;

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
        distinct_users
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
            ARRAY_LENGTH(user_habus_id, 1) AS distinct_users
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
                ANYARRAY_UNIQ(user_habus_id) AS user_habus_id 
        FROM
            (SELECT property, 
                    aw_area_ha, 
                    sum(iacs_area_ha) AS sum_iacs_area, 
                    sum(area_of_intersection) AS sum_intersection_area, 
                    (sum(area_of_intersection)/aw_area_ha) * 100 AS percent_intersection, 
                    ARRAY_AGG(llo ORDER BY llo) AS llo, 
                    sum(CASE WHEN llo='Y' then 1 END) AS llo_y,
                    SUM(CASE WHEN llo='N' then 1 END) AS llo_n,
                    ANYARRAY_AGG(owner_habus_id) AS owner_habus_id, 
                    ANYARRAY_AGG(user_habus_id) AS user_habus_id
            FROM ladss.aw_and_iacs_landownership_2018
            GROUP BY property, aw_area_ha) goo) foo) foobar;