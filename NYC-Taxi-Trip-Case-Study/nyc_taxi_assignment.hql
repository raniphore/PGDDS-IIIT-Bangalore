----------------------------
-- NYC TAXI DATA ASSIGNMENT
----------------------------

-- Title: The New York Taxi and Limousine Commission [TLC] Analysis
-- Brief: This assignment is centered on the concepts of Ingesting and Analyzing Big Data on the APACHE-HIVE platform.
-- The dataset provided contains the detailed trip level data of trips made by taxis in New York City.
-- Our analysis is focused on the yellow taxis for the months of November and December 2017.

-- Dataset Access: We can access the dataset using the below links:
-- Trip Data: http://upgrad-labs.cloudenablers.com:50003/filebrowser/download=/common_folder/nyc_taxi_data/yellow_tripdata_2017.csv
-- The Data Dictionary for this dataset is as follows: [Field Name--Description]
-- [1] vendorid--A code indicating the TPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc. 
-- [2] tpep_pickup_timestamp--The date and time when the meter was engaged.
-- [3] tpep_dropoff_timestamp--The date and time when the meter was disengaged.
-- [4] passenger_count--The number of passengers in the vehicle. This is a driver-entered value.
-- [5] trip_distance--The elapsed trip distance in miles reported by the taximeter.
-- [6] rate_code--The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride.
-- [7] store_forward_flag--This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka store and forward, because the vehicle did not have a connection to the server. Y= store and forward trip N= not a store and forward trip.
-- [8] pickup_location--TLC Taxi Zone in which the taximeter was engaged.
-- [9] dropoff_location--TLC Taxi Zone in which the taximeter was disengaged.
-- [10] payment_type--A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip.
-- [11] fare_charge--The time-and-distance fare calculated by the meter.
-- [12] extra_charge--Miscellaneous extras and surcharges.  Currently, this only includes the $0.50 and $1 rush hour and overnight charges.
-- [13] mta_tax_charge--$0.50 MTA tax that is automatically triggered based on the metered rate in use.
-- [14] tip_amount--Tip amount – This field is automatically populated for credit card tips. Cash tips are not included.
-- [15] tolls_charge--Total amount of all tolls paid in trip.
-- [16] improvement_surcharge--$0.30 improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.
-- [17] total_charge--The total amount charged to passengers. It does not include cash tips.

-----------------------
-- ENVIRONMENT SETUP
-----------------------

-- Executing commands for setting the environment

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

-------------------------------------------------------------------------------------------------------------------
------------------------- Stage 1 : Data Quality Check and Exploratory Data Analysis ------------------------------
-------------------------------------------------------------------------------------------------------------------

-- We will be using the custom database that has been created for this assignment. We will load the taxi data into an initial data table title nyc_taxi_data.
-- We will extract basic statistics from this nyc_taxi_data table and check for data validity and highlight any nonconforming values.
-- We will identify all the nonconformities in the initial data table nyc_taxi_data. We will refer the data dictionary for getting an idea on the non-conforming values.
-- Then create a neatly partitioned and formatted table to store the final data eliminating all the erroneous records, which will be used for Stage 2 and Stage 3 analysis.

-- 1.1 Using the database created
use ranip_hive;
show tables;

-- 1.2 Creating the initial table nyc_taxi_data for preliminary data analysis.
drop table nyc_taxi_data;

CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi_data
(
    vendorid int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count int,
    trip_distance double,
    ratecodeid int,
    store_and_fwd_flag string,
    pulocationid int,
    dolocationid int,
    payment_type int,
    fare_amount double,
    extra double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    improvement_surcharge double,
    total_amount double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION '/common_folder/nyc_taxi_data/'
tblproperties ("skip.header.line.count"="1");

-- 1.3 Basic sanity check for the data loaded

select * from nyc_taxi_data limit 10;

select count(*) from nyc_taxi_data; -- 1174569

-- All table fields have been populated with an appropriate schema and format to store the data

--------------------------------
---------- Question 1 ---------- 
--------------------------------

-- 1.4 How many records has each TPEP provider provided? Write a query that summarises the number of records of each provider.

select vendorid,count(1) as num_records from nyc_taxi_data group by vendorid order by vendorid;

-- Creative Moblie Technologies,LLC provided 527386 records [0.52 million records]
-- VeriFone Inc. provided 647183 records [0.64 million records]

--------------------------------
---------- Question 2 ---------- 
--------------------------------

-- 1.5 The data provided is for months November and December only. Check whether the data is consistent, and if not, identify the data quality issues.
-- Mention all data quality issues in comments.

-- Since both tpep_pickup_datetime and tpep_dropoff_timestamps are available we will set tpep_pickup_datetime as our reference column as it is the first point of contact with passenger.
-- Only trips that registered a tpep_pickup_datetime and tpep_dropoff_datetime during November and December 2017 will be considered.
-- This implies that only trips that have been started and completed between November to December 2017 will be considered for our analysis.

select year(tpep_pickup_datetime)as pickup_year, month(tpep_pickup_datetime)as pickup_month, count(*)as num_records
from nyc_taxi_data
group by year(tpep_pickup_datetime), month(tpep_pickup_datetime)
order by pickup_year, pickup_month;

-- 2003    1       1
-- 2008    12      2
-- 2009    1       1
-- 2017    10      6
-- 2017    11      580300
-- 2017    12      594255
-- 2018    1       4

-- The tpep_pickup_datetime results reveal several nonconforming records. The tpep_pickup_datetime range from the year 2003 to 2018.
-- Since our study is focused only on the trip details of November and December of 2017. There are a 14 nonconforming records based on tpep_pickup_datetime.

-- 1.6 Let us observe if there are any discrepancy in the tpep_dropoff_datetime.

select year(tpep_dropoff_datetime) as dropoff_year, month(tpep_dropoff_datetime) as dropoff_month, count(*) as num_records
from nyc_taxi_data
group by year(tpep_dropoff_datetime), month(tpep_dropoff_datetime)
order by dropoff_year, dropoff_month;

-- 2003    1       1
-- 2008    12      1
-- 2009    1       2
-- 2017    10      2
-- 2017    11      580053
-- 2017    12      594399
-- 2018    1       110
-- 2019    4       1

-- The tpep_dropoff_datetime results range from the year 2003 to 2019.
-- There are a total of 117 non-conforming records. 

-- 1.7 Let's check if there are any records in which the tpep_pickup_datetime is after the tpep_dropoff_datetime. This will clearly be a nonconformity as it is not logical.

SELECT count(*)
FROM nyc_taxi_data
where tpep_pickup_datetime > tpep_dropoff_datetime;

-- There are 73 erroneous records where pickup timestamp is greater than drop timestamp.

-- Clearly the dataset is not consistent and thus we will dig deeper and investigate every column individually to identify the non-conformities asscoiated with it.
-- We will validate each columns by referring the data dictionary.

-- 1.8 Check for discrepancy in column passenger count, since it is a driver entered value.

select passenger_count as num_of_passengers, count(*) as num_records
from nyc_taxi_data
group by passenger_count
order by passenger_count;

-- 0       6824
-- 1       827499
-- 2       176872
-- 3       50693
-- 4       24951
-- 5       54568
-- 6       33146
-- 7       12
-- 8       3
-- 9       1

-- From the above results we could see that the passenger_count values range between 0 and 9.
-- Trips can't be registered wih 0 passengers so it is definitely an abnormality in the data.
-- Also the maximum amount of passengers allowed in a yellow taxicab by law is four (4) in a four (4) passenger taxicab or five (5) passengers in a five (5) passenger taxicab, except that an additional passenger must be accepted if such passenger is under the age of seven (7) and is held on the lap of an adult passenger seated in the rear.
-- Source : https://www1.nyc.gov/site/tlc/passengers/passenger-frequently-asked-questions.page
-- So we will consider passenger count between 1 and 6 as valid records.

-- 1.9 Check for negative value of trip_distance

select count(1) from nyc_taxi_data where trip_distance < 0; -- No discrepancy

-- 1.10 Check for discrepancy in column ratecodeid
select ratecodeid, count(*) as num_records
from nyc_taxi_data
group by ratecodeid
order by ratecodeid;

-- We could see that there is a ratecodeid of value 99 which doesn't exist in the data dictionary.
-- These 9 records for ratecodeid 99 will be treated as erroneous records.

-- 1.11 Check for discrepancy in column store_and_fwd_flag

select store_and_fwd_flag, count(*) as num_records
from nyc_taxi_data
group by store_and_fwd_flag
order by store_and_fwd_flag;

-- There are only 2 store_and_fwd_flag parameter values [Y and N] which is inline with the specified limits.

-- 1.12 Check for discrepancy in column payment_type

select payment_type, count(*) as num_records
from nyc_taxi_data
group by payment_type
order by payment_type;

-- There are 4 distinct payment types which is in agreement with the data dictionary.

-- 1.13 Check for discrepancy in column fare_amount , check if any fare amount is negative.

select count(*) from nyc_taxi_data where fare_amount < 0; 

-- 558 records are having negative fare amount. We will consider the cases where fare amount is 0 since it might happen due to any discount/offer.

-- 1.14 Check extra charge attribute

select extra, count(*) as num_records
from nyc_taxi_data
group by extra
order by extra;

-- There are 14 distinct extra attribute values in the dataset ranging between -$10.6 and $4.8
-- However, the extra attribute can only take up $0.5 and $1 during rush hour and overnight, otherwise it is $0. Therefore, all other values will be treated as non-conformities.

-- 1.15 Check mta_tax attribute

select mta_tax, count(*) as num_records
from nyc_taxi_data
group by mta_tax
order by mta_tax;

-- There are 5 distinct mta_tax values in the dataset ranging between -$0.5 and $11.4.
-- The data dictionary specified that mta_tax of $0.5 is triggered based on metered rate in use. Therefore, it can only take up two values $0 or $0.5 all other values will be treated as non-conformities.

-- 1.16 Check for discrepancy in improvement_surcharge attribute

select improvement_surcharge, count(*) as num_records
from nyc_taxi_data
group by improvement_surcharge
order by improvement_surcharge;

-- There are 4 distinct values of improvement_surcharge ranging between -$0.3 and $1.
-- The improvement_surcharge of $0.3 began being levied on assessed trips at flagdrop from year 2015, this means that the improvement_surcharge can only take up $0 or $0.3 . All other values of improvement_surcharge will be treated as non-conformity

-- 1.17 Check for negative values of tip_amount

select count(1) from nyc_taxi_data where tip_amount < 0;

-- 4 records are there having negative tip_amount, which will be treated as erroneous.

-- 1.18 Check for negative values of tolls_amount

select count(1) from nyc_taxi_data where tolls_amount < 0;

-- 3 records are present with negative tolls_amount, which will be treated as erroneous.

-- 1.19 Check for negative values of total_amount

select count(1) from nyc_taxi_data where total_amount < 0;

-- 558 records are present with negative total_amount, which will be treated as erroneous.

--------------------------------
---------- Question 3 ---------- 
--------------------------------

-- 1.20 You might have encountered unusual or erroneous rows in the dataset.
-- Can you conclude which vendor is doing a bad job in providing the records using different columns of the dataset? 

select vendorid, count(*) as erroneous_records
from nyc_taxi_data
where 
(  
   year(tpep_pickup_datetime) != 2017 
or month(tpep_pickup_datetime) not in (11,12) 
or year(tpep_dropoff_datetime) != 2017 
or month(tpep_dropoff_datetime) not in (11,12) 
or tpep_pickup_datetime > tpep_dropoff_datetime 
or passenger_count not in (1,2,3,4,5,6) 
or trip_distance < 0
or ratecodeid not in (1,2,3,4,5,6) 
or payment_type not in (1,2,3,4,5,6) 
or fare_amount < 0 
or extra not in (0,0.5,1) 
or mta_tax not in (0,0.5) 
or tip_amount < 0
or tolls_amount < 0
or improvement_surcharge not in (0,0.3)
or total_amount < 0
)
group by vendorid
order by vendorid;

-- vendorid   erroneous_records
--        1                8713
--        2                3429

-- For VendorID 1: Creative Moblie Technologies,LLC
-- Number of Non-Conforming Records Provided: 8713 records
-- Total Records Provided [From Query --1.4]: 527386 records
-- Percentage Non-Conforming Records: 0.017%

-- For VendorID 2: VeriFone Inc.
-- Number of Non-Conforming Records Provided: 3429
-- Total Records Provided [From Query --1.4]: 647183 records
-- Percentage Non-Conforming Records: 0.005%

-- Clearly from the above statements it is clear that of the two vendors, VendorID 1: Creative Moblie Technologies,LLC is doing a bad job of providing records.

-------------------------------------------------------------------------------------------------------------------
-------------------------------- Stage 2 : Create Clean Partitioned ORC Table -------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- We will be creating a partitioned orc table which won't have the erroneous records identified in previous step.

-- 2.1 Setting Hive Parameters in case not already set.

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;

-- 2.2 Create the partitioned table by having partition on month and day since there are only two months data that needs to be considered.

drop table nyc_taxi_partitioned_orc;

create external table if not exists nyc_taxi_partitioned_orc
(
    vendorid int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count int,
    trip_distance double,
    ratecodeid int,
    store_and_fwd_flag string,
    pulocationid int,
    dolocationid int,
    payment_type int,
    fare_amount double,
    extra double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    improvement_surcharge double,
    total_amount double
)
partitioned by (month int, day int)
stored as orc location '/user/hive/warehouse/ranip/nyc_taxi_orc'
tblproperties ("orc.compress"="SNAPPY");

-- 2.3 Populating the partitioned table with the valid records.

INSERT OVERWRITE TABLE nyc_taxi_partitioned_orc partition (month,day)
SELECT
vendorid,
tpep_pickup_datetime,
tpep_dropoff_datetime,
passenger_count,
trip_distance,
ratecodeid,
store_and_fwd_flag,
pulocationid,
dolocationid,
payment_type,
fare_amount,
extra,mta_tax,
tip_amount,
tolls_amount,
improvement_surcharge,
total_amount,
month(tpep_pickup_datetime) as month,
day(tpep_pickup_datetime) as day
FROM nyc_taxi_data
WHERE year(tpep_pickup_datetime) = 2017 
AND month(tpep_pickup_datetime) in (11,12) 
AND year(tpep_dropoff_datetime) = 2017 
AND month(tpep_dropoff_datetime) in (11,12) 
AND tpep_pickup_datetime <= tpep_dropoff_datetime 
AND passenger_count in (1,2,3,4,5,6) 
AND trip_distance >= 0
AND ratecodeid in (1,2,3,4,5,6) 
AND payment_type in (1,2,3,4,5,6) 
AND fare_amount >= 0 
AND extra in (0,0.5,1) 
AND mta_tax in (0,0.5) 
AND tip_amount >= 0
AND tolls_amount >= 0
AND improvement_surcharge in (0,0.3)
AND total_amount >= 0;

-- 2.4 Basic sanity check on the partitioned table

select count(1) from nyc_taxi_partitioned_orc;
-- 1162427

select * from nyc_taxi_partitioned_orc limit 10;

-------------------------------------------------------------------------------------------------------------------
----------------------------- Stage 3 : Analysis on the ORC Table - Analysis Level-1 ------------------------------
-------------------------------------------------------------------------------------------------------------------

-- All analysis wil be performed using the filtered, partitioned and formatted table nyc_taxi_partitioned_orc subject to the predefined data assumptions.

--------------------------------
---------- Question 1 ---------- 
--------------------------------

-- 3.1 Compare the overall average fare per trip for November and December.

-- Let's check the average fare amount for these two months

select month, round(avg(fare_amount),2) as avg_fare
from nyc_taxi_partitioned_orc
WHERE month in (11,12)
group by month
order by month;

-- November Average fare: $12.96
-- December Average fare: $12.75
-- Therefore the Average fare recorded during November is 1.65% higher than the average fare_charge recorded in December.

--------------------------------
---------- Question 2 ---------- 
--------------------------------

-- 3.2 Explore the 'number of passengers per trip' - how many trips are made by each level of 'Passenger_count'? 
-- Do most people travel solo or with other people?

-- Let's have a look at how many trips are made by each level of passenger_count 

select passenger_count as num_of_passengers, count(*)as num_records
from nyc_taxi_partitioned_orc
group by passenger_count
order by passenger_count;

-- Let's compare if the passengers prefer to travel solo [i.e, passenger_count=1] or in groups [i.e, passenger_count [2-6]]

SELECT sum(CASE when passenger_count = 1 THEN 1 ELSE 0 END)as Num_Solo_Passenger_Trips, 
sum(CASE when passenger_count != 1 THEN 1 ELSE 0 END)as Num_Group_Passenger_Trips, 
round(100*sum(CASE when passenger_count = 1 THEN 1 ELSE 0 END)/count(*),3) as Solo_Trips_as_Percentage_of_Total_Trips
from nyc_taxi_partitioned_orc;

-- Number of trips with Solo Passengers: 824084
-- Number of trips with Group Passengers: 338343 
-- Percentage of trips with Solo Passengers w.r.t Total Number of trips: 70.893%
-- From the results it is clear that in 70.893% of all trips, people prefer to travel Solo.

--------------------------------
---------- Question 3 ---------- 
--------------------------------

-- 3.3 Which is the most preferred mode of payment?

-- Let's check the number of records for each payment_type

select payment_type as Payment_Mode, count(*) as Num_Records
from nyc_taxi_partitioned_orc
group by payment_type
order by Num_Records desc;

-- From the results of the above query it is clear that Credit_Card [payment_type=1] is the most preferred payment method followed by Cash [payment_type=2].

--------------------------------
---------- Question 4 ---------- 
--------------------------------

-- 3.4 What is the average tip paid per trip? Compare the average tip with the 25th, 50th and 75th percentiles and comment whether the ‘average tip’ is a representative statistic (of the central tendency) of 'tip amount paid'. 
-- Hint: You may use percentile_approx(DOUBLE col, p): Returns an approximate pth percentile of a numeric column (including floating point types) in the group.

-- In our data dictionary it is clearly stated that tip_amount is not recorded for cash payments and is default set to 0. We need to remove these fields before we compute the central tendency as these records are synonymous to missing records. Therefore we will remove all records where payment_type=2 [Cash Payments]

select 
round(avg(tip_amount),3) as Average_Tip, 
round(percentile_approx(tip_amount,0.25),3)as 25th_Percentile_Tip, 
round(percentile_approx(tip_amount, 0.50),3)as 50th_Percentile_Tip, 
round(percentile_approx(tip_amount, 0.75),3)as 75th_Percentile_Tip, 
count(distinct tip_amount)as Distinct_Tip_Amounts
from nyc_taxi_partitioned_orc
where payment_type != 2;

-- Average Tip : $2.697 , 25th Percentile Tip : 1.32 , 50th Percentile Tip : 2.0 , 75th Percentile Tip : 3.05, Distinct tip amount : 2118

-- Here, since tip_amount is stored as double data type we have to use percentile_approx() instead of percentile(). From the documentation: percentile_approx(DOUBLE col, p [, B]) .Returns an approximate pth percentile of a numeric column (including floating point types) in the group. The B parameter controls approximation accuracy at the cost of memory. Higher values yield better approximations, and the default is 10,000. When the number of distinct values in col is smaller than B, this gives an exact percentile value.
-- Since the number of distinct tip amounts 2118 <10000 percentile_approx() returns the exact percentile value.
-- Here $0.697 difference of the Average_Tip - Median_Tip [50th percentile], this diffence constitutes to 40.2% of the inter-quartile range. Therefore, there is significant skewness in the distribution of the tip_amount parameter. This implies that the Average Tip is sqewed to the right of the Median_tip. This may be offset due to certain records having higher tip_amount values. Therefore, in this situation Average_Tip is not representative of central tendency. We can consider Median_Tip as a better representative of central tendency.

--------------------------------
---------- Question 5 ---------- 
--------------------------------

-- 3.5 Explore the ‘Extra’ (charge) variable - what fraction of total trips have an extra charge is levied?

select extra as extra_misc_charge, count(*)as num_records
from nyc_taxi_partitioned_orc
group by extra
order by extra;

-- The number of trips where the extra_charge was levied is marginally lower than the number of trips for which it was not. 

select
sum(CASE when extra != 0 THEN 1 ELSE 0 END) as Trips_With_Extra_Misc_Charge,
count(*) as Total_Number_Trips,
round(sum(CASE when extra != 0 THEN 1 ELSE 0 END)/count(*),4) as Fraction_Trips_With_Extra_Charge
from nyc_taxi_partitioned_orc;

-- Number of Trips for which the Extra_Misc_Charge was levied: 534891
-- Total Number of Trips: 1162427
-- Fraction of trips for which the Extra_Misc_Charge was levied: 0.4602 [or 46.02%]

-------------------------------------------------------------------------------------------------------------------
----------------------------- Stage 4 : Analysis on the ORC Table - Analysis Level-2 ------------------------------
-------------------------------------------------------------------------------------------------------------------

--------------------------------
---------- Question 1 ---------- 
--------------------------------

-- 4.1 What is the correlation between the number of passengers on any given trip, and the tip paid per trip? Do multiple travellers tip more compared to solo travellers? 
-- Hint: Use CORR(Col_1, Col_2)

-- Here we are trying to perform a correlation study between the tip_amount and number of passengers. Since this study will be directly impacted with the magnitude value of tip_amount and our dataset encodes tip_amount as $0 for all trips that are paid with Cash or with [payment_type=2] irrespective of the number of passengers. This will distort the correlation value. Therefore, we need to exclude the records with payment_type=2 for this query.

select
round(corr(passenger_count, tip_amount),3)as Corr_PassengerCnt_vs_TipAmt,
round(avg(CASE when passenger_count=1 then tip_amount else null end),3) as Solo_Trips_Average_Tip,
round(avg(CASE when passenger_count != 1 then tip_amount else null end),3) as Group_Trips_Average_Tip
from nyc_taxi_partitioned_orc
where payment_type != 2;

-- Correlation between Passenger Count and Tip_Amount: +0.008
-- This suggests a very weak positive correlation between Passenger Count and Tip_Amount.
-- Average Tip for Solo Trips: $2.674
-- Average Tip for Group Trips: $2.756

-- Passengers travelling in groups are likely to give a higher tip.

--------------------------------
---------- Question 2 ---------- 
--------------------------------

-- 4.2 Segregate the data into five segments of 'tip paid': [0-5), [5-10), [10-15) , [15-20) and >=20.
-- Calculate the percentage share of each bucket (i.e. the fraction of trips falling in each bucket).

select 
tip_bucket,
count(*) as num_records_per_bucket,
max(records_count) as total_num_records,
round(count(*)/max(records_count),4)as tip_bucket_asfractionof_overall
from
(
select count(*) over () records_count,
CASE when tip_amount >= 0 and tip_amount <5 then 'Bucket_0_5' 
     when tip_amount >=5 and tip_amount < 10 then 'Bucket_5_10'
     when tip_amount >=10 and tip_amount < 15 then 'Bucket_10_15'
     when tip_amount >=15 and tip_amount < 20 then 'Bucket_15_20'
     else 'Bucket_above_20' 
end as tip_bucket 
from nyc_taxi_partitioned_orc
)a
group by tip_bucket
order by tip_bucket_asfractionof_overall desc;

-- tip_bucket     num_records_per_bucket     total_num_records    tip_bucket_asfractionof_overall
   -----------    ----------------------     -----------------    -------------------------------
-- Bucket_0_5                    1073233               1162427                             0.9233
-- Bucket_5_10                     65480               1162427                             0.0563
-- Bucket_10_15                    20200               1162427                             0.0174
-- Bucket_15_20                     2315               1162427                              0.002
-- Bucket_above_20                  1199               1162427                              0.001

-- The results of the table clearly specify the following about tip buckets:

-- Bucket_0_5 constitutes 92.33% of all records in the dataset.
-- Bucket_5_10 constitutes 5.63% of all records in the dataset.
-- Bucket_10_15 constitutes 1.74% of all records in the dataset.
-- Bucket_15_20 constitutes 0.2% of all records in the dataset.
-- Bucket_above_20 constitutes 0.1% of all records in the dataset.

--------------------------------
---------- Question 3 ---------- 
--------------------------------

-- 4.3 Which month has a greater average 'speed' - November or December? Note that the variable 'speed' will have to be derived from other metrics.
-- Hint: You have columns for distance and time.

-- We will convert the timestamp into epoch time and do the subtraction to get the time taken for each trip in seconds.

SELECT 
round(avg(CASE when month = 11 THEN trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600) ELSE NULL end),4) as November_Average_Speed_MPH,
round(avg(CASE when month = 12 THEN trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600) ELSE NULL end),4) as December_Average_Speed_MPH,
round(avg(CASE when month = 11 THEN trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600) ELSE NULL end),4) - round(avg(CASE when month = 12 THEN trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600) ELSE NULL end),4) as November_minus_December_Avg_Speed_MPH
from nyc_taxi_partitioned_orc;

-- November Month Average Speed: 10.9475 MPH
-- December Month Average Speed: 11.046 MPH
-- Average Speed of November - Average Speed of December: -0.098 MPH
-- The Average Speed of taxis in December is greater than their Average Speed in November.

--------------------------------
---------- Question 4 ---------- 
--------------------------------

-- 4.4 Analyse the average speed of the most happening days of the year, i.e. 31st December (New year's eve) and 25th December (Christmas) and compare it with the overall average.

SELECT
round(avg(CASE when month=12 and day=25 THEN (trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)) ELSE null end),4) as ChristmasEve_Average_Speed_MPH,
round(avg(CASE when month=12 and day=31 THEN (trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)) ELSE null end),4) as NewYearEve_Average_Speed_MPH,
round(avg((trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600))),4) as Overall_Average_Speed_MPH,
round(round(avg(CASE when month=12 and day=25 THEN (trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)) ELSE null end),4) - round(avg((trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600))),4),4) as ChristmasEve_minus_Overall_Avg_Speed_MPH,
round(round(avg(CASE when month=12 and day=31 THEN (trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600)) ELSE null end),4) - round(avg((trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime))/3600))),4),4) as NewYearEve_minus_Overall_Avg_Speed_MPH
from nyc_taxi_partitioned_orc;

-- Overall Average Speed for November and December Combined: 10.997 MPH

-- 1. Average Speed Statistics of Christmas Eve (25th December)
-- Average Speed on Christmas Eve: 15.2423 MPH
-- Speed greater than Overall Avg: 4.245 MPH
-- Percentage greater than Overall Avg: + 38.6%

-- 2. Average Speed Statistics of New Year's Eve (31st December)
-- Average Speed on New Year's Eve: 13.2318 MPH
-- Speed greater than Overall Avg: 2.235 MPH
-- Percentage greater than Overall Avg: + 20.32%

-- The average speed on both Cristmas and New Year is higher than the overall average speed.
-- However, the average speed compared to overall avg speed is higher for Christmas Eve compared to New Year's Eve.

