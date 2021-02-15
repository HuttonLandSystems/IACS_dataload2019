# 2019 IACS Data Load
Gianna Gandossi | The James Hutton Institute | 12/09/2019 to 23/01/2020

*   Edited 15/02/2021 to fix integer values in user_bps_claimed_area and user_land_use_area. Updated counts of affected rows are in the script at https://github.com/HuttonLandSystems/IACS_dataload2019/blob/master/main_data_load_from_raw.sql.

## Introduction
This document focuses on the Permanent and Seasonal Land Parcels tables representing SAF claims and declarations on land parcels in Scotland. The Permanent table is for those businesses who own and sometimes lease the land out to users, or renters, which are represented in the Seasonal table. 

## Data Access 
In September 2019, LADSS received a DVD of the new IACS data. The following table outlines the data received and the early integration and pseudonymization of the data at Hutton undertaken by Douglas Wardell-Johnson. Tables of the same data but of different years were combined with a new column added for year.

Table 1: Tables received and integrated
| Tables Received                                              | Tables integrated and pseudonymised at Hutton     |
|--------------------------------------------------------------|---------------------------------------------------|
| Common Grazing 2016 to 2019                                  |
| common\_grazing\_lpid\_detail\_effective\_date\_15\_05\_2016 |
| common\_grazing\_lpid\_detail\_effective\_date\_15\_05\_2017 |
| common\_grazing\_lpid\_detail\_effective\_date\_15\_05\_2018 |
| common\_grazing\_lpid\_detail\_effective\_date\_15\_05\_2019 | common\_grazing\_lpid\_detail\_deliv20190911      |
| SAF16\_Common\_Grazing\_Share\_Usage                         |
| SAF17\_Common\_Grazing\_Share\_Usage                         |
| SAF18\_Common\_Grazing\_Share\_Usage                         |
| SAF19\_Common\_Grazing\_Share\_Usage                         | saf\_common\_grazing\_share\_usage\_deliv20190911 |
| JHI Attribute Data                                           |
| Entitlement\_Year\_2016                                      |
| Entitlement\_Year\_2017                                      |
| Entitlement\_Year\_2018                                      |
| Entitlement\_Year\_2019                                      | entitlement\_year\_deliv20190911                  |
| Scheme\_Payment\_2016                                        |
| Scheme\_Payment\_2017                                        |
| Scheme\_Payment\_2018                                        | scheme\_payment\_deliv20190911                    |
| LPID\_field\_LFA\_Classifications\_Payment\_Regions          |
| LPID\_field\_LFA\_Classifications                            |
| lpid\_payment\_region                                        | lpid\_field\_lfa\_classifications\_deliv20190911  |
| lpid\_payment\_region\_deliv20190911                         |
| SAF\_Land\_Parcels\_Usage                                    |
| SAF16\_Permanent\_Land\_Parcels                              |
| SAF16\_Seasonal\_Land\_Parcels                               |
| SAF17\_Permanent\_Land\_Parcels                              |
| SAF17\_Seasonal\_Land\_Parcels                               |
| SAF18\_Permanent\_Land\_Parcels                              |
| SAF18\_Seasonal\_Land\_Parcels                               |
| SAF19\_Permanent\_Land\_Parcels                              |
| SAF19\_Seasonal\_Land\_Parcels                               | saf\_permanent\_land\_parcels\_deliv20190911      |
| saf\_seasonal\_land\_parcels\_deliv20190911                  |
| SAF\_Scheme\_Info                                            |
| SAF16\_Scheme\_Info                                          |
| SAF17\_Scheme\_Info                                          |
| SAF18\_Scheme\_Info                                          |
| SAF19\_Scheme\_Info                                          | saf\_scheme\_info\_deliv20190911                  |
| \[Spatial Data\]                                             |
| LPIS\_LAND\_PARCELS\_2018\_JHI                               |
| LPIS\_LAND\_PARCELS\_2019\_JHI                               |
| snapshot\_2017\_0417                                         | lpis\_land\_parcels\_2018\_jhi\_deliv20190911     |
| lpis\_land\_parcels\_2019\_jhi\_deliv20190911                |
| snapshot\_2017\_0417\_peudonymised                           |


## Pseudonymisation
Data fields which included personally identifiable information were pseudonymised in order to reduce the risks of a meaningful data breach and to fulfil Hutton’s data compliance and GDPR obligations. The newly generated Hutton IDs are incrementing integers which link to the IACS data fields in separate encrypted tables available only to the appointed data manager, Douglas Wardell-Johnson.

The two original tables have row counts of 3,376,660 (permanent) and 317,497 (seasonal). The data lives within pgLADSS, a PostgreSQL database system hosted internally on abpgs01.hutton.ac.uk, in a schema named rpid. Table 2 lists the matching structures of the two tables and describes their class and meaning. As no metadata accompanied the original data transfer, many of the descriptions listed in the table below are my conjecture.

| **Column Name** | **Data Class** | **Description** |
|---|---|---|
| mlc\_hahol\_id | integer | Pseudonymised\. Main location holding code\.  |
| habus\_id | integer | Pseudonymised\. Business Reference Number\. |
| hahol\_id | integer | Pseudonymised\. Holding code\. |
| hapar\_id | integer | Pseudonymised\. Parcel ID/FID/LPID |
| land\_parcel\_area | double | The area of the parcel in hectares\. |
| bps\_eligible\_area | double | The area of the parcel which is eligible under the Basic Payment Scheme in hectares\. |
| bps\_claimed\_area | double | The area of the parcel which is claimed under the BPS in hectares\. This is the number on which RPID bases their payments\. |
| verified\_exclusion | double | The area of the parcel which has an excluded land\_use in hectares\. |
| land\_use\_area | double | The area of the parcel which is being used under a land\_use in hectares\. |
| land\_use | text | A code \(typically three letters\) which declares the current use of the land\.  |
| land\_activity | text | A standard input indicator of whether the field is undergoing production or alternative practices\. |
| application\_status | text | An indicator of application status of a particular claim or declaration in the RPID process\. |
| land\_leased\_out | Boolean | Y/N flag field which indicates whether the land is leased out\. I assume if field is ‘Y’, business is owner of the land, even if land is in seasonal table\. Sometimes seasonal declarations have LLO for reasons unknown\.  |
| lfass\_flag | Boolean | Y/N flag field which indicates whether the land\_use\_area is classified as LFASS\. |
| is\_perm\_flag | Boolean | Y/N flag field which indicates whether the record exists in the permanent table or the seasonal table\. This column is not preserved through the Hutton integration because permanent and seasonal fields are combined where possible\. |
| claim\_id | text | Column added by Hutton\. An incrementing integer to track back to the original data if necessary\. A P \(for permanent\) or S \(for seasonal\) is added in front of the integer to differentiate the declarations’ origins\.  |
| year | integer | The year the claim or declaration was filed\. |
| organic\_status | Boolean | Eliminated\. Supposed to Indicate whether the parcel’s particular land\_use\_area is farmed organically\. All entries in this column were ‘N’ for No which does not reflect the reality of Scotland’s organic farms\.  |
| payment\_region | integer | Eliminated\. Enumerates one of three payment scheme regions indicating a measure of land suitability and LFASS grazing categories\. |

## Data Integration and Quality Control
### Exploratory Data Analysis 
The next phase in the process involves understanding and analysing the data tables through a process of curiosity-driven, iterative querying with SQL. The original data does not include metadata to describe RPID’s data acquisition and validation processes, structures, definitions, workflows, etc. This makes understanding the root cause of errors in the data difficult to ascertain. 

We know that RPID’s main concern is to verify claims made by farmers which affect their subsequent payments. Within the data tables, it seems there are only three fields which are used to verify these claims: bps_eligible_area, bps_claimed_area and verified_exclusion. It seems these fields may be prepopulated before a farmer fills in the SAF because of the fields’ consistency. 
 
However, for the Hutton to use this data for future analysis, it has to undergo a process of cleaning to correct errors and redundant data. The following sections enumerate the errors found and processes undertaken by which to fix them.

The scripts are included in a repository with associated files of the data load process at https://github.com/HuttonLandSystems/IACS_dataload2019/blob/master/main_data_load_from_raw.sql.

### Preliminary Data Elimination
From the very first steps, it was obvious there were some straightforward errors with the data which could be quickly eliminated. 

#### payment_region
The first problem encountered involved the payment_region column. In speaking with RPID, we found that their automated system of data intake misinterpreted multi-region split parcels or slivers between parcels and then created another record of the same data within different regions. This process created 55,250 (permanent) and 4,362 (seasonal) duplicate records. These records were deleted and the payment_region column was eliminated. 

#### organic_status
Every entry in the organic_status column equals ‘N’, which is not reflective of reality and so the column was eliminated.

#### application_status
It was obvious from looking at the data that those records categorised as ‘Wait for Deadline/Inspection’ were erroneous and so were deleted. 

Table 3: Six possible application_status entries and their counts
| application\_status           | Permanent count | Seasonal count |
|-------------------------------|-----------------|----------------|
| Application ready for payment | 2,628,879       | 251,124        |
| Under Action/Assessment       | 432,303         | 33,630         |
| Wait for Deadline/Inspection  | 308,305         | 32,217         |
| Submitted                     | 6,838           | 523            |
| Selected for QMC              | 207             | 3              |
| Wait for Land Change          | 128             | 0              |

#### 2019 data 
The data for year 2019 has yet to be validated, paid and/or the farms inspected. This was verified by the application_status column for which the values included those in the following table, none of which are a final payment-ready status. Because this data has not been verified, the 2019 data was eliminated. 

Table 4: Count of declarations from 2019 and their application_status
| application\_status          | Permanent count | Seasonal count |
|------------------------------|-----------------|----------------|
| Under Action/Assessment      | 403,834         | 31,157         |
| Wait for Deadline/Inspection | 271,039         | 29,145         |
| Submitted                    | 6,838           | 523            |


#### NULL data 
The following records which were missing key data were eliminated. 

Table 5: NULL quantifiers in SQL and their counts 
| NULL quantifier                                                                                                | Permanent count | Seasonal count |
|----------------------------------------------------------------------------------------------------------------|-----------------|----------------|
| \(land\_use\_area IS NULL OR land\_use\_area = 0\) AND \(land\_use = ‘EXCL’ OR land\_use = ‘DELETED LANDUSE’\) | 720,741         | 76,649         |
| land\_use = ‘’                                                                                                 | 1,761           | 707            |
| hapar\_id IS NULL                                                                                              | 131             | 35             |


#### Data Cleaning
land_parcel_area
The first port of call was to fix instances in the data where (of which there were only 30 records between the two datasets): land_parcel_area IS NULL OR land_parcel_area = 0  
I fixed this problem in three steps:
1.	Infer land_parcel_area from same hapar_id in different year (8 total)
2.	Infer land_parcel_area from land_use_area in single claim row (14 total)
3.	Delete remaining NULL land_parcel_area (20 total)

#### land_use_area
The next step was to fix instances where: land_use_area IS NULL OR land_use_area = 0
There were considerably more of these that the previous: 6,310 (permanent) and 2,640 (seasonal).
Solutions:
1.	Copy land_parcel_area for single claims where land_parcel_area = bps_eligible_area (1,530 total)
2.	Copy land_use_area values from same land_use claims in different years (186 total)
3.	Adjust bps_claimed_area to match land_parcel_area where bps_claimed_area > land_parcel_area (709 total)
4.	Adjust land_use_area to match bps_claimed_area (207,568 total)
    *   This is the most radical change in the code. As land_use_area is (probably) not verified or validated in the data collection process, there is nothing that ties this value down to anything. Hence the use of bps_claimed_area as the real value of used area in a parcel.
5.	Delete remaining NULL land_use_area (1,517 total)

### Joins
To combine owners and renters into the same table with a direct relationship, I had to do a series of four joins with the data based on decreasing specificity:
1.	Join on hapar_id, year, land_use and land_use_area (33,481 rows)
2.	Join on hapar_id, year and land_use (16,447 rows) 
3.	Join on hapar_id, year and land_use_area (3,373 rows) 
4.	Join on hapar_id and year (9,773 rows)

Then a series of corrections from superfluous joins, namely the fourth (most general) join. The deletions are laid out in the following table: 

Table 6: SQL queries which find superfluous fourth joins and their counts
| Superfluous fourth joins                                                                                     | Joined count |
|--------------------------------------------------------------------------------------------------------------|--------------|
| owner\_land\_use IN \(SELECT land\_use FROM excl\) AND user\_bps\_claimed\_area <> 0                         | 1,364        |
| user\_land\_use IN \(SELECT land\_use FROM excl\) AND owner\_bps\_claimed\_area <> 0                         | 483          |
| owner\_land\_use NOT IN \(SELECT land\_use FROM excl\) AND user\_land\_use IN \(SELECT land\_use FROM excl\) | 2,152        |


All joined claims or declarations which had survived the process (59,558) were moved to a new ‘joined’ temporary table and deleted from the working tables so as not confuse the following process. 

### Mutually Exclusive 
1,842,703 remaining permanent rows and 115,100 seasonal rows which did not successfully join in the previous process are assumed to be mutually exclusive, i.e. those owners who are either not leasing out their land or are leasing out to non-SAF renters AND renters leasing from non-SAF owners. 

A note is added to the record if this is the case and the NULL land_use, either owner or renter, is set to NON_SAF where the land_leased_out flag indicates ‘Y’ for owners OR where there is no associated owner declaration and the user land use is NOT an excluded land use. (A table with all the excluded land uses is included at the end of this document.)

## Final Clean up 
The last steps of the process aim to combine all the remaining rows while minimising data loss or alteration. Within the ‘joined’ temporary table, several records required certain assumptions: 

*   Where owner_land_parcel_area <> user_land_parcel_area, I assume the larger area
    *   Changes 346 rows with largest change = 5.77 ha and total change = 79.63 ha
*   Where owner_bps_eligible_area <> user_bps_eligible_area, I assume the larger area 
    *   Changes 414 rows with largest change = 273.3 ha and total change = 797.27 ha
*   Where owner_verified_exclusion <> user_verified_exclusion, I assume the larger area
    *   Changes 4,670 rows with largest change = 4,208.76 ha and total change = 50,680.66 ha
*   Where owner_land_activity <> user_land_activity, I assume user knows best
    *   Changes 40,793 rows
*   Where owner_application_status <> user_application_status, I assume that the status is ‘Under Action/Assessment’ takes priority over any other status
    *   Changes 1,789 rows

Several excluded land use declarations seemed to be automatically generated, and these were deleted. Specifically, those with zero land_use_area and an excluded land_use: 33,800 rows.

The final step in the process includes making land_parcel_area match for the same hapar_ids in the same year. This adjusted 889 rows with the largest area difference being 2.35 ha, although 98.6% of the records are affected by less than 0.5 ha area difference. 

The following table describes the data structure of the final combined table: 

Table 7: Final cleaned and combined data structure 
| Column Name               | Data Class | Description                                                                                                                                                                                                                   |
|---------------------------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| owner\_mlc\_hahol\_id     | integer    | Pseudonymised\. Main location holding code from permanent table\.                                                                                                                                                             |
| user\_mlc\_hahol\_id      | integer    | Pseudonymised\. Main location holding code from seasonal table\.                                                                                                                                                              |
| owner\_habus\_id          | integer    | Pseudonymised\. Business Reference Number from permanent table\.                                                                                                                                                              |
| user\_habus\_id           | integer    | Pseudonymised\. Business Reference Number from seasonal table\.                                                                                                                                                               |
| owner\_hahol\_id          | integer    | Pseudonymised\. Holding code from permanent table\.                                                                                                                                                                           |
| user\_hahol\_id           | integer    | Pseudonymised\. Holding code from seasonal table\.                                                                                                                                                                            |
| hapar\_id                 | integer    | Pseudonymised\. Parcel ID/FID/LPID                                                                                                                                                                                            |
| land\_parcel\_area        | double     | The area of the parcel in hectares\.                                                                                                                                                                                          |
| bps\_eligible\_area       | double     | The area of the parcel which is eligible under the Basic Payment Scheme in hectares\.                                                                                                                                         |
| owner\_bps\_claimed\_area | double     | The area of the parcel which is claimed under the BPS in hectares from permanent table\.                                                                                                                                      |
| user\_bps\_claimed\_area  | double     | The area of the parcel which is claimed under the BPS in hectares from seasonal table\.                                                                                                                                       |
| verified\_exclusion       | double     | The area of the parcel which has an excluded land\_use in hectares\.                                                                                                                                                          |
| owner\_land\_use\_area    | double     | The area of the parcel which is being used under a particular land\_use in hectares from the permanent table\.                                                                                                                |
| user\_land\_use\_area     | double     | The area of the parcel which is being used under a particular land\_use in hectares from the seasonal table\.                                                                                                                 |
| owner\_land\_use          | text       | A code \(typically three letters\) which declares the current use of the land from the permanent table\.                                                                                                                      |
| user\_land\_use           | text       | A code \(typically three letters\) which declares the current use of the land from the seasonal table\.                                                                                                                       |
| land\_activity            | text       | A standard input indicator of whether the field is undergoing production or alternative practices\.                                                                                                                           |
| application\_status       | text       | An indicator of application status of a particular claim or declaration in the RPID process\.                                                                                                                                 |
| owner\_land\_leased\_out  | Boolean    | Y/N flag field which indicates whether the land is leased out from the permanent table\.                                                                                                                                      |
| user\_land\_leased\_out   | Boolean    | Y/N flag field which indicates whether the land is leased out from the seasonal table\.                                                                                                                                       |
| owner\_lfass\_flag        | Boolean    | Y/N flag field which indicates whether the land\_use\_area is classified as LFASS from the permanent table\.                                                                                                                  |
| user\_lfass\_flag         | Boolean    | Y/N flag field which indicates whether the land\_use\_area is classified as LFASS from the seasonal table\.                                                                                                                   |
| claim\_id                 | text       | Column added by Hutton\. An incrementing integer to track back to the original data if necessary\. A P \(for permanent\) or S \(for seasonal\) is added in front of the integer to differentiate the declarations’ origins\.  |
| year                      | integer    | The year the claim or declaration was filed\.                                                                                                                                                                                 |
| change\_note              | text       | Column added by Hutton\. A descriptive field to document changes to data\. At each change, a new string is added separated by a semi\-colon\.                                                                                 |
| error\_log                | text       | Column added by Hutton\. A descriptive field enumerating errors which remain in the data\.                                                                                                                                    |
## Metrics
Problems remain with the data and much effort has been expended to record these. 

Table 8 describes the top ten change_note occurrences in the final table. There are another 355 combinations of different change_note occurrences not shown in the table, all of which occur with less than 0.1% frequency.

Table 8: Top ten change_note occurrences and their frequencies
| \# | Change\_note                                                                                                                                     | Count     | %     |
|----|--------------------------------------------------------------------------------------------------------------------------------------------------|-----------|-------|
| 1  | \[NULL\]                                                                                                                                         | 1,655,749 | 83\.5 |
| 2  | adjust owner land\_use\_area to match bps\_claimed\_area;                                                                                        | 184,274   | 9\.3  |
| 3  | infer non\-SAF owner;                                                                                                                            | 71,595    | 3\.6  |
| 4  | first join; owner and user land\_activity choice based on assumption user knows best;                                                            | 15,235    | 0\.8  |
| 5  | adjust user land\_use\_area to match bps\_claimed\_area; infer non\-SAF owner;                                                                   | 14,650    | 0\.7  |
| 6  | first join;                                                                                                                                      | 7,318     | 0\.4  |
| 7  | infer non\-SAF renter;                                                                                                                           | 6,688     | 0\.3  |
| 8  | second join; owner and user land\_activity choice based on assumption user knows best;                                                           | 4,484     | 0\.2  |
| 9  | second join; adjust user land\_use\_area to match bps\_claimed\_area; owner and user land\_activity choice based on assumption user knows best;  | 3,600     | 0\.2  |
| 10 | second join;                                                                                                                                     | 2,336     | 0\.1  |


Table 9 describes the top ten error_log occurrences in the final table. Many of these notes are not necessarily a problem. For example, owner_land_use <> user_land_use does not indicate a problem but rather a specification between owner and renter for excluded land use or an PGRS to RGR relationship. There are another 115 combinations of different error_log occurrences not shown in the table, all of which occur with less than 0.1% frequency.

Table 9: Top ten error_log occurrences and their frequencies
| \# | Error\_log                                                                                                                                                                       | Count     | %     |
|----|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|-------|
| 1  | \[NULL\]                                                                                                                                                                         | 1,859,135 | 93\.7 |
| 2  | owner\_land\_use <> user\_land\_use;                                                                                                                                             | 91,802    | 4\.6  |
| 3  | sum\(bps\_claimed\_area\) > land\_parcel\_area;                                                                                                                                  | 10,977    | 0\.6  |
| 4  | sum\(land\_use\_area\) > land\_parcel\_area;                                                                                                                                     | 5,748     | 0\.3  |
| 5  | multiple seasonal businesses declaring on same hapar\_id; owner\_land\_use <> user\_land\_use; sum\(land\_use\_area\) > land\_parcel\_area;                                      | 2,019     | 0\.1  |
| 6  | doubled owner claims in join \(many to one\); owner\_land\_use <> user\_land\_use; sum\(land\_use\_area\) > land\_parcel\_area;                                                  | 1,565     | 0\.1  |
| 7  | owner\_land\_use <> user\_land\_use; sum\(bps\_claimed\_area\) > land\_parcel\_area;                                                                                             | 1,382     | 0\.1  |
| 8  | doubled owner claims in join \(many to one\); owner\_land\_use <> user\_land\_use; sum\(land\_use\_area\) > land\_parcel\_area; sum\(bps\_claimed\_area\) > land\_parcel\_area;  | 1,358     | 0\.1  |
| 9  | doubled owner claims in join \(many to one\); sum\(land\_use\_area\) > land\_parcel\_area;                                                                                       | 1,271     | 0\.1  |
| 10 | doubled owner claims in join \(many to one\); sum\(land\_use\_area\) > land\_parcel\_area; sum\(bps\_claimed\_area\) > land\_parcel\_area;                                       | 970       | 0\.0  |


Table 10: Number of spatial parcels which have at least one owner or user associated 
| Year | Cleaned Data Count | Original Data Count | %     |
|------|--------------------|---------------------|-------|
| 2016 | 444,827            | 448,840             | 99\.1 |
| 2017 | 444,113            | 449,556             | 98\.8 |
| 2018 | 439,580            | 451,774             | 97\.3 |


Table 11: Comparison of digi_area of spatial data to the declared land_parcel_area in the data
| Year | < 0.01 difference |      | < 0.1 difference |     | > 0.1 difference |     |
|------|-------------------|------|------------------|-----|------------------|-----|
|      | Count             | %    | Count            | %   | Count            | %   |
| 2016 | 396,898           | 89.2 | 18,565           | 4.2 | 29,364           | 6.6 |
| 2017 | 399,330           | 89.9 | 17,309           | 4.9 | 27,474           | 6.2 |
| 2018 | 400,474           | 91.1 | 13,665           | 3.1 | 25,441           | 5.8 |


## Final Notes
- The best indicator for land use area for a parcel is bps_claimed_area.
- There are many declarations which have zero bps_claimed_area. In these cases, the best indicator of land user area for a parcel is the land_use_area column.
 
## Acronyms
| BPS   | Basic Payment Scheme                         |
|-------|----------------------------------------------|
| BRN   | Business Reference Number                    |
| CAP   | Common Agricultural Policy                   |
| DPIA  | Data Protection Impact Assessment            |
| FID   | Field Identifier                             |
| GDPR  | General Data Protection Regulations          |
| IACS  | Integrated Administration and Control System |
| LADSS | Land Allocation Decision Support System      |
| LFASS | Less Favoured Area Support Scheme            |
| LLO   | Land Leased Out                              |
| LPID  | Land Parcel Identification                   |
| LPIS  | Land Parcel Identification System            |
| MLC   | Main Location Code                           |
| RPID  | Rural Payments and Inspection Division       |
| SAF   | Single Application Form                      |
| SQL   | Structured Query Language                    |


## Excluded land uses 
| Land use code | Description                |
|---------------|----------------------------|
| BLU\-GLS      | Blueberries \- glasshouse  |
| BRA           | Bracken                    |
| BUI           | Building                   |
| EXCL          | Generic exclusion          |
| FSE           | Foreshore                  |
| GOR           | Gorse                      |
| LLO           | Land let out               |
| MAR           | Marsh                      |
| RASP\-GLS     | Raspberries \- glasshouse  |
| ROAD          | Road                       |
| ROK           | Rocks                      |
| SCB           | Scrub                      |
| SCE           | Scree                      |
| STRB\-GLS     | Strawberries \- glasshouse |
| TOM\-GLS      | Tomatoes \- glasshouse     |
| TREE          | Trees                      |
| TREES         | Trees                      |
| WAT           | Water                      |



