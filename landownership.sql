SELECT hapar_id,
       STRING_AGG(CAST(owner_habus_id AS TEXT), '; ') AS owner_habus_id,
       STRING_AGG(user_habus_id, '; ') AS user_habus_id,
       CASE
           WHEN STRING_AGG(llo, '') LIKE '%Y%' THEN 'Y'
           ELSE 'N'
       END AS llo
FROM
    (SELECT hapar_id,
            owner_habus_id,
            STRING_AGG(CAST(user_habus_id AS TEXT), '; ') AS user_habus_id,
            CASE
                WHEN STRING_AGG(llo, '') LIKE '%Y%' THEN 'Y'
                ELSE 'N'
            END AS llo
     FROM
         (SELECT hapar_id,
                 owner_habus_id,
                 user_habus_id,
                 CASE
                     WHEN STRING_AGG(CASE
                                         WHEN (CASE
                                                   WHEN owner_llo IS NULL THEN user_llo
                                                   WHEN user_llo IS NULL THEN owner_llo
                                                   ELSE CONCAT(owner_llo, user_llo)
                                               END) LIKE '%Y%' THEN 'Y'
                                         ELSE 'N'
                                     END, '') LIKE '%Y%' THEN 'Y'
                     ELSE 'N'
                 END AS llo
          FROM
              (SELECT hapar_id_v2 AS hapar_id,
                      owner_habus_id,
                      user_habus_id,
                      owner_land_leased_out AS owner_llo,
                      user_land_leased_out AS user_llo
               FROM ladss.saf_iacs_2018_processed
               GROUP BY hapar_id_v2,
                        owner_habus_id,
                        user_habus_id,
                        owner_land_leased_out,
                        user_land_leased_out) foo
          GROUP BY hapar_id,
                   owner_habus_id,
                   user_habus_id) bar
     GROUP BY hapar_id,
              owner_habus_id) foobar
GROUP BY hapar_id