# Project - Data Warehouse

## Objective

A hands-on data engineering project built as part of the Udacity Data Engineering with AWS course, focusing on designing and implementing a cloud-based data warehouse using Amazon Redshift.

## How to run the Project

1. Create Redshift cluster; Make sure the cluster is publicily accessible
2. Enter CLUSTER and IAM_ROLE info in dwh.cfg file
3. Execute python create_tables.py
4. Execute python etl.py

## What happens

1. When you execute create_tables.py. The following tables get created.

    Staging Tables
        staging_events
        staging_songs

    Fact Table
        songplays

    Dimension Tables
        users
        songs
        artists
        time

2. When you execute etl.py, 
    - first staging tables will be filled using data from S3.
    - then fact and dimension tables will be filled using data from staging tables.

## Observations

### Errors Occured

1. Error occured during loading data into staging_songs table due to column length defined for artist_name, artist_location and title was not enough. I had to increase it to 500.

2. The same artist_id has different artist names, so I had to use SQL window function to get rid of duplicate records while inserting data into artists (dimension) table.

## Data loading status

staging_events = 2s
staging_songs = more than 60mins


## Stats

### Number of records in each table

| Table           | Result  | Query Used                           |
|-----------------|--------:|--------------------------------------|
| staging_events  |   8056  | SELECT count(1) FROM staging_events; |
| staging_songs   | 385252  | SELECT count(1) FROM staging_songs;  |
| songplays       |   6962  | SELECT count(1) FROM songplays;      |
| users           |    105  | SELECT count(1) FROM users;          |
| songs           | 384995  | SELECT count(1) FROM songs;          |
| artists         |  30542  | SELECT count(1) FROM artists;        |
| time            |   6813  | SELECT count(1) FROM time;           |

## Analytics

### Top 5 paid users

#### Query
SELECT sp.user_id, u.first_name, u.last_name, COUNT(*) AS total_count 
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id 
AND sp.level = u.level
WHERE sp.level = 'paid'
GROUP BY sp.user_id, u.first_name, u.last_name 
ORDER BY total_count DESC 
LIMIT 5;

#### Result

| User ID | First Name | Last Name | Level | Total Count |
|--------:|------------|-----------|-------|------------:|
|      80 | Tegan      | Levine    | paid  |         671 |
|      49 | Chloe      | Cuevas    | paid  |         652 |
|      97 | Kate       | Harrell   | paid  |         552 |
|      15 | Lily       | Koch      | paid  |         483 |
|      44 | Aleena     | Kirby     | paid  |         417 |


### Top 5 free users

#### Query

SELECT sp.user_id, u.first_name, u.last_name, COUNT(*) AS total_count 
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id 
AND sp.level = u.level
WHERE sp.level = 'free'
GROUP BY sp.user_id, u.first_name, u.last_name 
ORDER BY total_count DESC 
LIMIT 5;

#### Result

| User ID | First Name | Last Name | Level | Total Count |
|--------:|------------|-----------|-------|------------:|
|      26 | Ryan       | Smith     | free  |         120 |
|      32 | Lily       | Burns     | free  |          59 |
|      86 | Aiden      | Hess      | free  |          51 |
|     101 | Jayden     | Fox       | free  |          49 |
|      50 | Ava        | Robinson  | free  |          46 |


### Most popular 10 Songs and its artists

#### Query

SELECT s.title AS song_title, a.name AS artist_name, COUNT(*) AS total_count
FROM songplays sp 
JOIN songs s ON sp.song_id = s.song_id 
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY song_title, artist_name
ORDER BY total_count DESC
LIMIT 10;

#### Result

| Song Title | Artist Name | Total Count |
|------------|------------|------------|
| Greece 2000 | 3 Drives On A Vinyl | 55 |
| You're The One | Dwight Yoakam | 37 |
| Stronger | Kanye West | 28 |
| Revelry | Kings Of Leon | 27 |
| Yellow | Coldplay | 24 |
| Sehr kosmisch | Harmonia | 21 |
| Bring Me To Life | Evanescence | 21 |
| Horn Concerto No. 4 in E flat K495: II. Romance (Andante cantabile) | Barry Tuckwell/Academy of St Martin-in-the-Fields/Sir Neville Marriner | 19 |
| Secrets | OneRepublic | 17 |
| Canada | Five Iron Frenzy | 17 |


### Most popular weekday

#### Query

SELECT weekday, count(start_time) as count
FROM time
GROUP BY weekday
ORDER BY count DESC
LIMIT 1;

#### Result

| Weekday | Count |
|--------:|------:|
|       3 |  1361 |