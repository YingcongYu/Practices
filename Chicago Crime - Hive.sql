### csv
CREATE EXTERNAL TABLE ken_db.chicago_crime_19_20(
    id  bigint,
    case_number string,
    `date`  string,
    block   string,
    iucr    string,
    primary_type    string, 
    description     string,
    loc_desc    string,
    arrest      boolean,
    domestic    boolean,
    beat        string,
    district    string,
    ward        int,
    community_area  string,
    fbi_code        string,
    x_coordinate    int,
    y_coordinate    int,
    yr              int,
    updated_on      string,
    latitude        float,
    longitude       float,
    loc             string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
    'seprateorChar' = ',',
    'quoteChar' = '\"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/chicago_2019_2020'
TBLPROPERTIES('skip.header.line.count' = '1');

SELECT * FROM ken_db.chicago_crime_19_20;



### parquet
CREATE EXTERNAL TABLE chicago_crime_19_20_par(
    id  bigint,
    case_number string,
    `date`  string,
    block   string,
    iucr    string,
    primary_type    string, 
    description     string,
    loc_desc    string,
    arrest      boolean,
    domestic    boolean,
    beat        string,
    district    string,
    ward        int,
    community_area  string,
    fbi_code        string,
    x_coordinate    int,
    y_coordinate    int,
    yr              int,
    updated_on      string,
    latitude        float,
    longitude       float,
    loc             string
)
STORED AS PARQUETFILE
LOCATION '/user/ken/data/chicago_2019_2020_par';

INSERT into ken_db.chicago_crime_19_20_par SELECT * from ken_db.chicago_crime_19_20;



###parquet with date from string to bigint
CREATE EXTERNAL TABLE chicago_crime_19_20_par2_date(
    id  bigint,
    case_number string,
    `date`  BIGINT
)
STORED AS PARQUETFILE
LOCATION '/user/ken/data/chicago_2019_2020_par2_date';

INSERT into ken_db.chicago_crime_19_20_par2_date SELECT id, case_number, UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy hh:mm:ss aa') from ken_db.chicago_crime_19_20;

SELECT * FROM chicago_crime_19_20_par2_date;



### calculate arrest ratio by using sub-query
SELECT arrested.yr, arrested.arrest_count/total.total_count as ratio from
(SELECT yr, count(*) as arrest_count FROM chicago_crime_19_20
WHERE arrest = "true"
GROUP BY yr) as arrested
inner join
(SELECT yr, count(*) as total_count FROM chicago_crime_19_20
GROUP BY yr) as total
on arrested.yr = total.yr;



### parquet with date from string to bigint, arrest&domestic from string to boolean
CREATE EXTERNAL TABLE chicago_crime_19_20_par(
    id  bigint,
    case_number string,
    `date`  BIGINT,
    block   string,
    iucr    string,
    primary_type    string, 
    description     string,
    loc_desc    string,
    arrest      boolean,
    domestic    boolean,
    beat        string,
    district    string,
    ward        int,
    community_area  string,
    fbi_code        string,
    x_coordinate    int,
    y_coordinate    int,
    yr              int,
    updated_on      string,
    latitude        float,
    longitude       float,
    loc             string
)
STORED AS PARQUETFILE
LOCATION '/user/ken/data/chicago_2019_2020_par';

INSERT into ken_db.chicago_crime_19_20_par
SELECT id, case_number, UNIX_TIMESTAMP(`date`, 'MM/dd/yyyy hh:mm:ss aa'),
block, iucr, primary_type, description, loc_desc, if(arrest='true', TRUE, FALSE), if(domestic='true', TRUE, FALSE),
beat, district, ward, community_area, fbi_code, x_coordinate, y_coordinate, yr, updated_on, latitude, latitude, loc
FROM ken_db.chicago_crime_19_20;



### calculting ratio based on new parquet
SELECT yr, sum(if(arrest=true, 1, 0))/count(*)
FROM chicago_crime_19_20_par
GROUP BY yr;



### Find out which crime type has lowest arrest/crime ratio for each year.
SELECT arrested.arrest_count/total.total_count as ratio, total.yr, total.primary_type from
(SELECT yr, primary_type, count(*) as arrest_count
FROM chicago_crime_19_20_par
WHERE arrest = true
GROUP BY yr, primary_type) as arrested
INNER JOIN
(SELECT yr, primary_type, count(*) as total_count
FROM chicago_crime_19_20_par
GROUP BY yr, primary_type) as total
on arrested.yr = total.yr and arrested.primary_type = total.primary_type
order by ratio;



### minimum ratio for each year
SELECT min(year_type_ratio.ratio), year_type_ratio.yr from
(SELECT arrested.arrest_count/total.total_count as ratio, total.yr as yr, total.primary_type from
(SELECT yr, primary_type, count(*) as arrest_count
FROM chicago_crime_19_20_par
WHERE arrest = true
GROUP BY yr, primary_type) as arrested
INNER JOIN
(SELECT yr, primary_type, count(*) as total_count
FROM chicago_crime_19_20_par
GROUP BY yr, primary_type) as total
on arrested.yr = total.yr and arrested.primary_type = total.primary_type
order by ratio) as year_type_ratio
GROUP BY year_type_ratio.yr
ORDER BY year_type_ratio.yr;



### minimum ratio for each year in which type
select min_year, type, min_ratio from
(select min(year_type_ratio.ratio) as min_ratio,year_type_ratio.yr as min_year from
(select arrested.arrest_count/total.total_count as ratio,total.primary_type as type,total.yr as yr from
    (select yr,primary_type,count(*) as arrest_count FROM chicago_par
    where arrest = true
    GROUP BY yr, primary_type) as arrested
    join
    (select yr,primary_type,count(*) as total_count FROM chicago_par
    GROUP BY yr, primary_type) as total
    on arrested.yr = total.yr and arrested.primary_type = total.primary_type
    order by ratio) as year_type_ratio
group by year_type_ratio.yr
order by year_type_ratio.yr) as year_min_ratio
join
(select arrested.arrest_count/total.total_count as ratio,total.primary_type as type,total.yr as yr from
    (select yr,primary_type,count(*) as arrest_count FROM chicago_par
    where arrest = true
    GROUP BY yr, primary_type) as arrested
    join
    (select yr,primary_type,count(*) as total_count FROM chicago_par
    GROUP BY yr, primary_type) as total
    on arrested.yr = total.yr and arrested.primary_type = total.primary_type) as year_type_ratio
on
min_year = yr and min_ratio = ratio ;
