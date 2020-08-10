# MEWS Data Task Solution

The premium hotel prospers with crafting personalized experience to each guest. The hotel manager wants to learn more about the customers and asks to analyse reservations of his hotel.
Using only SQL, the task is to report on the following:

- What are the popular choices of booking rates (table `rate`, columns `ShortRateName` or `RateName`) for different segments of customers (table `reservation`, columns `AgeGroup`, `Gender`, `NationalityCode`)?

- What are the typical guests who do online check-in? Is it somehow different when you compare reservations created across different weekdays (table `reservation`, `IsOnlineCheckin` column)?

The briefing is a shortened version of the [data analyst task crafted by Mews Systems](https://github.com/MewsSystems/developers/tree/master/DataScience#mews-data-analyst-task).

[ ] data disclaimer / I direcly refer to source of the data.

## Assets

Two CSV files `rates` and `reservations` that you can load into SQL database, or you can use `import_mssql.sql` file that contains the same data in the form of queries to create the db tables and insert all the data.

All asset rights belong to Mews Systems. The data is used to exemplify SQL queries.

[ ] add links to my gist
[ ] Continue here
The data was imported into local SQL database with tables "rate" and "reservation".

### 1) What are the popular choices of booking rates (table `rate`, columns `ShortRateName` or `RateName`) for different segments of customers (table `reservation`, columns `AgeGroup`, `Gender`, `NationalityCode`)?

Initially, we explore the given data by categories to decide direction of the analysis.

```sql
SELECT
COUNT(*) AS reservations_count
from reservation
```

reservations_count|
-|
2501|

```sql
SELECT
gender,
COUNT(*) AS reservations_count
FROM reservation
GROUP BY gender
```

gender|reservations_count
-|-
0|846
1|1295
2|360

We proceed with an assumption that the value **0** for `gender` means that the value was not specified by client. Without additional instructions, we do not know value for males and females and we will refer to genders as gender **1** and gender **2**.

```sql
SELECT
AgeGroup AS age_group,
COUNT(*) AS reservations_count
FROM reservation
GROUP BY reservation.AgeGroup
ORDER BY 1
```

age_group|reservations_count
-|-
0|1520
25|234
35|279
45|241
55|146
65|65
100|16

Similarly to gender `age_group` **0** is not provided data. `age_group` of **100** deserves a frowny look as outlier and will be excluded from the analysis.  

```sql
SELECT
NationalityCode AS nationality_code,
COUNT(*) AS reservations_count
FROM reservation
GROUP BY reservation.NationalityCode
ORDER BY 2 DESC
```

nationality_code|reservations_count
-|-
NULL|1096
US|243
GB|187
DE|154
SK|72
CZ|67
CN|59
RU|51
IT|48
FR|46
...|...

The ouput for countries is shown for top 10 results and, overall, we have 71 unique value of `nationality_code`. Noticeably, **NULL** values cover around 40% of reservations.

#### `Gender and Booking Rate`

With an overview of categories data ready, we proceed to find clients segments for rates. Starting with `gender`, above we identified that value **0** should be excluded from the analysis. The left join returns all rows from `reservations` whether or not there is a matching row in the `rate`. **OVER (PARTITION BY)** window function calculates total reservations per gender and percentage of different room rates for gender.

```sql
SELECT
reservation.gender AS reservation_gender,
rate.RateName AS rate_rate_name,
COUNT(*) AS reservations_count,
SUM(COUNT(*)) OVER (
    PARTITION BY
    reservation.gender
    ) AS gender_total_reservations,
100*COUNT(*)/SUM(COUNT(*)) OVER (
    PARTITION BY
    reservation.gender
    ) AS rate_share_per_gender
FROM reservation
LEFT JOIN rate ON reservation.RateId = rate.RateId
WHERE
 reservation.gender != 0

GROUP BY reservation.gender, rate.RateName
ORDER BY reservation.gender, reservations_count DESC
```

reservation_gender|rate_rate_name|reservations_count|gender_total_reservations|rate_share_per_gender
-|-|-|-|-
1|**Fully Flexible**|753|1295|**58**
...|...|...|...|...
2|**Fully Flexible**|169|360|**46**
...|...|...|...|...

For both genders _Fully Flexible_ rate is by far the most popular.

#### `AgeGroup and Booking Rate`

With very similar query we look into preferences of room rates by age brackets.Age gorups of **0** and **100** are excluded.

```sql
SELECT
reservation.agegroup AS reservation_agegroup,
rate.RateName AS rate_rate_name,
COUNT(*) AS reservations_count,
SUM(COUNT(*)) OVER (
    PARTITION BY
    reservation.agegroup
    ) AS agegroup_total_reservations,
100*COUNT(*)/SUM(COUNT(*)) OVER (
    PARTITION BY
    reservation.agegroup
    ) AS rate_share_per_agegroup
FROM reservation
LEFT JOIN rate ON reservation.RateId = rate.RateId
WHERE
 reservation.agegroup NOT IN (0,100)

GROUP BY reservation.agegroup, rate.RateName
ORDER BY reservation.agegroup, reservations_count DESC
```

reservation_agegroup|rate_ratename|reservations_count|agegroup_total_reservations|rate_share_per_agegroup
-|-|-|-|-
25|Fully Flexible|103|234|44
25|Non Refundable BAR BB|57|234|24
...|...|...|...|...
35|Fully Flexible|142|279|50
35|Non Refundable BAR BB|57|279|20
...|...|...|...|...
45|Fully Flexible|127|241|52
45|Non Refundable BAR BB|41|241|17
...|...|...|...|...
55|Fully Flexible|78|146|53
55|Non Refundable BAR BB|19|146|13
55|Early - 21 days|18|146|12
...|...|...|...|...
65|Fully Flexible|36|65|55
65|Non Refundable BAR BB|9|65|13
65|Early - 21 days|8|65|12
...|...|...|...|...

Among all age brackets _Fully Flexible_ and _Non Refundable BAR BB_ room rates are the most popular. For older people, _Early - 21 days_ gains in the reservation share.

#### `NationalityCode and Booking Rate`

We want to avoid countries with too few bookings and include sub-query to select countries with more than 50 bookings (criteria is based on the list of top 10 countries by reservations and out of all 2500 bookings, 50 is 2%).

```sql
SELECT
reservation.NationalityCode AS reservation_nationality_code,
rate.RateName AS rate_rate_name,
COUNT(*) AS reservations_count,
SUM(COUNT(*)) OVER (
    PARTITION BY
    reservation.NationalityCode
    ) AS country_total_reservations,
100*COUNT(*)/SUM(COUNT(*)) OVER (
    PARTITION BY
    reservation.NationalityCode
    ) AS rate_share_per_agegroup
FROM reservation
LEFT JOIN rate ON reservation.RateId = rate.RateId
-- pick countries with 50+ bookings
WHERE
    reservation.NationalityCode IN
    (SELECT
    NationalityCode
    FROM reservation
    GROUP BY NationalityCode
    HAVING COUNT(*) >= 50)

GROUP BY reservation.NationalityCode, rate.RateName
ORDER BY reservation.NationalityCode, reservations_count DESC
```

The table below presents summary of the query and shows dominance of _Fully Flexible_ rate for several countries and _Early - 21 days_ has visible presence in **CN**.

NationalityCode|RateName|Share of reservations in country
-|-|-
CZ, DE, GB, SK|Fully Flexible|60%+
CN, RU, US|Fully Flexible|40%+
CN|Early - 21 days|30%

<!-- [ ] Summary for the chapter  -->

### 2) What are the typical guests who do online check-in? Is it somehow different when you compare reservations created across different weekdays (table `reservation`, `IsOnlineCheckin` column)?

To find out about typical guests, we shall start with content of the column `IsOnlineCheckin`.

```sql
SELECT  IsOnlineCheckin,
COUNT(*) AS reservations_count
FROM reservation
GROUP BY IsOnlineCheckin
ORDER BY 2 DESC
```

IsOnlineCheckin|reservations_count
-|-
0|2353
1|148

From the discussion above, we conclude that **1** corresponds to reservation with online check-in. Under 5% of users used online check-in in the booking process.

#### `Gender and Online Checkin`

```sql
SELECT
reservation.gender AS reservation_gender,
COUNT(*) AS reservations_count,
SUM(COUNT(*)) OVER () As total_reservations,
100*count(*)/sum(count(*)) OVER () AS reservations_share_per_gender
FROM reservation
WHERE
IsOnlineCheckin = 1
AND reservation.gender != 0

GROUP BY reservation.gender
ORDER BY reservation_gender
```

reservation_gender|reservations_count|total_reservations|reservations_share_per_gender
-|-|-|-
1|119|148|80
2|29|148|19

Gender **1** dominates demographics of users who checked-in online. We continue examination for this sex only.

#### `AgeGroup and Online Checkin for Gender 1`

```sql
SELECT
reservation.agegroup AS reservation_agegroup,
COUNT(*) AS reservations_count,
SUM(COUNT(*)) OVER () As total_reservations,
100*count(*)/sum(count(*)) OVER () AS reservations_share_per_agegroup
FROM reservation
WHERE
IsOnlineCheckin = 1
AND reservation.gender = 1
AND reservation.agegroup NOT IN (0,100)

GROUP BY reservation.agegroup
ORDER BY 1
```

reservation_agegroup|reservations_count|total_reservations|reservations_share_per_agegroup
-|-|-|-
25|25|117|**21**
35|35|117|**29**
45|29|117|**24**
55|23|117|**19**
65|5|117|**4**

The distribution among age brackets is uniform with the exception of **65** `agegroup`.

#### `NationalityCode and Online Checkin for Gender 1`

```sql
SELECT
reservation.NationalityCode AS reservation_nationality_code,
COUNT(*) AS checkin_count,
AVG(country_reservations.country_sum) AS total_reservations_nationality
FROM reservation
LEFT JOIN (
    --subquery to calculate country totals
    SELECT
    NationalityCode,
	COUNT(*) AS country_sum
	FROM reservation
	GROUP BY NationalityCode
	) AS country_reservations
	ON reservation.NationalityCode = country_reservations.NationalityCode
WHERE
reservation.IsOnlineCheckin = 1
AND reservation.gender = 1

GROUP BY reservation.NationalityCode
ORDER BY total_reservations_nationality DESC
```

reservation_nationality_code|checkin_count|total_reservations_nationality
-|-|-
US|18|243
GB|17|187
DE|6|154
...|...|...

No country has a significant share of online reservations.

#### `Impact of booking weekday on customer segment which prefers online checkin`

So far we have identified that clients with `gender` **1** and `agegroup` within **(25,35,45,55)** use option of online checkin most often. In order to examine if booking weekday impacts this customer segment, we will look for total number of online checkins by booking day of the week.

```sql
SET DATEFIRST 1

SELECT
weekday_segment_checkin.*,
weekday_all_checkin.checking_all_count
FROM (
-- segmented online checkins per weekday
SELECT
DATEPART(weekday, reservation.CreatedUtc) AS weekday_datepart,
COUNT(reservation.CreatedUtc) AS checkin_segment_count
FROM reservation
WHERE
reservation.IsOnlineCheckin = 1
AND reservation.gender = 1
AND reservation.agegroup NOT IN (0, 65, 100)
GROUP BY DATEPART(weekday, CreatedUtc)
) AS weekday_segment_checkin
-- all online checkins per weekday
INNER JOIN  
 (SELECT
    DATEPART(weekday, reservation.CreatedUtc) AS weekday_datepart,
	COUNT(reservation.CreatedUtc) AS checking_all_count
	FROM reservation
	WHERE
	reservation.IsOnlineCheckin = 1
GROUP BY DATEPART(weekday, CreatedUtc)
)AS weekday_all_checkin
	ON weekday_segment_checkin.weekday_datepart = weekday_all_checkin.weekday_datepart
```

weekday_datepart|checkin_segment_count|checking_all_count
--|--|--
1|17|26
2|21|27
3|17|24
4|17|24
5|15|17
6|14|18
7|11|12

Share of reservations with online checkins done by the defined segment constitutes around 75% of all bookings with online checkins regardless of customer type. The defined customer segment holds accross different weekdays.

----
Extra notes
From the first sight it might appear that number of online checkins decreases toward weekend. Evaluating online checkins and all reservations for `gender` **1** and `agegroup` **25, 35, 45, 55**, it comes clear high share of online checkins for bookings done on Saturday and Sunday (weekdays 6 and 7 respectively).

Is it somehow different when you compare reservations created across different weekdays (table `reservation`, `IsOnlineCheckin` column)?

Our search for customer segments moves to booking day of the week category.

A quick examination of online check-ins in terms of agegroup AND order weekdays does not hold any trends.
