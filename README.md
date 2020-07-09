# MEWS Data Task Solution

The task itself is located at [MewsSystems depository](https://github.com/MewsSystems/developers/tree/master/DataScience#mews-data-analyst-task).

The data was imported into local SQL database with tables "rate" and "reservation".

## Part 1 SQL Queries for Tech Lead

1) What are the popular choices of booking rates (table `rate`, columns `ShortRateName` or `RateName`) for different segments of customers (table `reservation`, columns `AgeGroup`, `Gender`, `NationalityCode`)?

Initially, we explore the given data by categories to decide direction of the analysis.

```sql
SELECT
COUNT(*) AS reservations_count
from reservation
```

reservations_count|
---
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

The ouput for countries is shown for top 10 results and, overall, we have 71 unique value of `nationality_code`. Noticeably, **NULL** values cover around 40% of reservations.

With an overview of categories data ready, we proceed to find clients segments for rates. Starting with `gender`, above we identified that values **0** and **100** should be excluded from the analysis.

```sql
SELECT
Res.gender,
Rat.RateName,
count(*) As 'Reservations',
sum(count(*)) over (partition by gender) AS 'GenderTotal',
100*count(*)/sum(count(*)) over (partition by gender) AS 'RateShare'
from reservation Res left join rate Rat
on Res.[RateId] = Rat.[RateId]
where Res.gender != 0

group by Res.gender, Rat.RateName
order by Res.gender, Reservations desc

```


Segment by Gender - code

<!-- [ ] mention number of unique values for countries
 [ ] write about what join and why we are using
 -->





> part 1 
> part 2 