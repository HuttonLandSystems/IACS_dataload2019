-- finds difference between bps_eligible_area and total payment_regions
SELECT cg_hahol_id,
       hapar_id,
       YEAR,
       digitised_area,
       excluded_land_area,
       bps_eligible_area,
       region_total,
       bps_eligible_area - region_total AS diff
FROM
    (SELECT cg_hahol_id,
            hapar_id,
            YEAR,
            digitised_area,
            bps_eligible_area,
            excluded_land_area,
            payment_region_1 + payment_region_2 + payment_region_3 AS region_total
     FROM rpid.common_grazing_lpid_detail_deliv20190911) foo
WHERE bps_eligible_area <> region_total

