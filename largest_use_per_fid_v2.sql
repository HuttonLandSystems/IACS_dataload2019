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
          WHERE owner_land_use NOT IN
                  (SELECT land_use
                   FROM excl)
              AND user_land_use NOT IN
                  (SELECT land_use
                   FROM excl)) foo)
SELECT hapar_id,
       land_use,
       used_area,
       ROW_NUMBER() OVER (PARTITION BY hapar_id,
                                       used_area
                          ORDER BY used_area DESC, rank)
FROM
    (SELECT hapar_id,
            land_use,
            SUM(used_area) AS used_area
     FROM cte
     GROUP BY hapar_id,
              land_use) bar
JOIN lu_rank USING (land_use)