DROP TABLE IF EXISTS ladss.saf_iacs_landownership_2018;


SELECT hapar_id,
       owner_habus_id,
       user_habus_id,
       llo,
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
                         WHERE error_log NOT LIKE '%commons%'
                         GROUP BY hapar_id_v2,
                                  owner_habus_id,
                                  user_habus_id,
                                  owner_land_leased_out,
                                  user_land_leased_out) foo
                    GROUP BY hapar_id) bar) foobar) foobar2
     ORDER BY hapar_id) foobar3
JOIN ladss.saf_iacs_2018_processed_fields ON hapar_id = hapar_id_v2;


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
       brns_per_fid INTO ladss.aw_and_iacs_landownership_2018
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
                 ST_CollectionExtract(geom, 3) AS aw_geom
          FROM ladss.aw_landownership_2015) aw
     JOIN
         (SELECT area_ha AS iacs_area_ha,
                 hapar_id,
                 owner_habus_id,
                 user_habus_id,
                 llo,
                 brns_per_fid,
                 ST_CollectionExtract(geom, 3) AS iacs_geom
          FROM ladss.saf_iacs_landownership_2018) iacs ON ST_INTERSECTS(aw_geom, iacs_geom)
     WHERE code <> 'SU042') goo;


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
       brns_per_fid INTO ladss.aw_and_iacs_landownership_2018_sample
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
                 ST_CollectionExtract(geom, 3) AS aw_geom
          FROM ladss.aw_landownership_2015) aw
     JOIN
         (SELECT area_ha AS iacs_area_ha,
                 hapar_id,
                 owner_habus_id,
                 user_habus_id,
                 llo,
                 brns_per_fid,
                 ST_CollectionExtract(geom, 3) AS iacs_geom
          FROM ladss.saf_iacs_landownership_2018) iacs ON ST_INTERSECTS(aw_geom, iacs_geom)
     WHERE code = 'AB002'
         OR code = 'AB170'
         OR code = 'AR444'
         OR code = 'CA073'
         OR code = 'DM002'
         OR code = 'EL027'
         OR code = 'IN138'
         OR code = 'IN337'
         OR code = 'IN762'
         OR code = 'PR076'
         OR code = 'PR102'
         OR code = 'RC213'
         OR code = 'RC613'
         OR code = 'RX001'
         OR code = 'SU233' ) goo