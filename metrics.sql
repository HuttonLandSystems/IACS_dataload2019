-- categorises parcel area vs land use area
SELECT CASE
           WHEN land_parcel_area > sum_lua THEN 'Parcel > LU'
           WHEN land_parcel_area = sum_lua THEN 'Equal'
           WHEN land_parcel_area < sum_lua THEN 'Parcel < LU'
           ELSE 'Other'
       END AS Area_comparison,
       COUNT(1)
FROM
     (SELECT hapar_id,
             SUM(land_use_area) OVER (PARTITION BY hapar_id,
                                                   year) AS sum_lua,
                                     land_parcel_area,
                                     year
      FROM temp_permanent) foo
GROUP BY 1
ORDER BY count DESC;

Equal	     1,542,209
Parcel > LU	349,954
Parcel < LU	6,112

(10809/1959847)*100 = 0.55% <-- permanent

Equal	     127,451
Parcel > LU	39,547
Parcel < LU	5,085

(5440/180233)*100 = 3.01% <-- seasonal

--* finds and sums difference in SINGLE overclaims where bps_claimed_area > land_parcel_area for joined table
SELECT hapar_id, YEAR, total_claimed_area - owner_land_parcel_area AS diff
FROM
    (SELECT hapar_id,
            YEAR,
            owner_land_parcel_area,
            user_land_parcel_area,
            owner_bps_claimed_area + user_bps_claimed_area AS total_claimed_area
     FROM joined) foo
WHERE total_claimed_area > owner_land_parcel_area
    OR total_claimed_area > user_land_parcel_area
    ORDER BY diff DESC;


 -- categorises llo flags based on matches or no matches
SELECT CASE
           WHEN perm_llo = 'N'
                AND seas_llo = 'Y' Then 'N-Y no match'
           WHEN perm_llo = 'Y'
                AND seas_llo = 'N' THEN 'Y-N no match'
           WHEN perm_llo = 'N'
                AND seas_llo = 'N' THEN 'N match'
           WHEN perm_llo = 'Y'
                AND seas_llo = 'Y' THEN 'Y match'
           WHEN perm_llo = seas_llo THEN 'Match'
           ELSE 'Other'
       END as LLO_flag_compare,
       Count(1)
FROM
     (SELECT hapar_id,
             year,
             perm.land_leased_out AS perm_llo,
             seas.land_leased_out AS seas_llo
      FROM temp_permanent AS perm
      INNER JOIN temp_seasonal AS seas USING (hapar_id,
                                              land_use,
                                              land_use_area,
                                              year)
      GROUP BY hapar_id,
               year,
               perm_llo,
               seas_llo) foo
GROUP BY 1
ORDER BY LLO_flag_compare
 
-- Where total land use area matches but number of claims doesnt ON JOIN
SELECT DISTINCT perm_s.hapar_id
FROM (
           ( SELECT hapar_id,
                    SUM(land_use_area) lua_per,
                    COUNT(*) AS cnt_per,
                    land_parcel_area
            FROM temp_permanent
            WHERE YEAR = 2018
            GROUP BY hapar_id,
                     land_parcel_area) perm_s
      INNER JOIN
           ( SELECT hapar_id,
                    SUM(land_use_area) lua_sea,
                    COUNT(*) AS cnt_sea,
                    land_parcel_area
            FROM temp_seasonal
            WHERE YEAR = 2018
            GROUP BY hapar_id,
                     land_parcel_area) seas_s ON seas_s.hapar_id = perm_s.hapar_id
      AND lua_sea = lua_per)
WHERE cnt_per != cnt_sea

--See changes made by count
SELECT change_note,
       COUNT(*)
FROM combine
GROUP BY change_note
ORDER BY count DESC