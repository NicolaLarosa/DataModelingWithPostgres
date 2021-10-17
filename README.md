# Data Modeling with Postgres
## Project overview:
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Purpose of the project:
The purpose of this project is to create a Postgres database able to store data in tables designed to optimize queries on song play analysis, and to analyze data based on songs and user activity, which currently are stored in several json files stored in specific folders. The Sparkify analytics team is particularly interested in understanding what songs users are listening to, having all the data stored in the DB, will help them to retrieve useful insights.

## ER Diagram:
<img src="ER Diagram.PNG" alt="Markdown Monster icon" style="float: left; margin-right: 150px;" />

For this project, it has benen choosen the **Fact and Dimension schema**:
1. *songplays* is the **FACT** table.
2. *songs*, *artist*, *users*, *time* arec the **DIMENSION** tables.

The **Fact and Dimension schema** has been choosen because there is one **FACT** table (*songplays*) which contains columns that easily (without using complex queries) allow to join it to other **DIMENSIONS** tables such as *songs*, *artist*, *users*, *time*. In order to allow the Sparkify analytics team to query those tables, creating one table containing all the info needed to build up operational reports or for example using this table as input dataset for a dashboard.
In addition to this, in this project the **Fact and Dimension schema** can handle and allow for ONE to MANY relations due the fact there are few **DIMENSIONS** tables (*songs*, *artist*, *users*, *time*) that are directly related with the **FACT** table (*songplays*).

## Project files:
1. **create_tables.py** its main targets are to connect to the DB, create and drop tables if not already exists.
2. **etl.py** its main targets are to load all the json files, then to process the data coming from those files.
3. **sql_queries.py** its main target is to provide all the DDL and DML queries.
4. **test.ipynb** its main target is to test and run *sql_queries.py*, *create_tables.py*, *etl.py* and to doublecheck tables, previously created and filled through the pipeline, quering them.

## ETL Pipeline:
The ETL pipeline is made by three .py files:

1. **sql_queries.py** contains the following queries:
    1. DDL queries to DROP or CREATE tables in the DB. There's one DROP and CREATE query for each table (*songplays*, *songs*, *artist*, *users*, *time*) of the DB.
    2. DML queries to INSERT or SELECT (i.e. song_select query) records in the DB. There's one INSERT query for each table (*songplays*, *songs*, *artist*, *users*, *time*) of the DB.
2. **create_tables.py** contains the following functions:
    1. create_database, which its main target is to connect to sparkify database.
    2. drop_tables, which drops each table using the queries in `drop_table_queries` list contained in *sql_queries.py*.
    3. create_tables, which creates each table using the queries in `create_table_queries` list contained in *sql_queries.py*.
    4. main, which call inside is body: create_database, drop_tables, create_tables and finally close the connection to the DB.
3. **etl.py** contains the following functions:
    1. process_data, which loads all the json files (coming from these file paths: data/song_data, data/log_data) and call process_song_file or process_log_file (according to which file path has been choosen) to start processing the files.
    2. process_song_file, which aims to load all the records of the files coming from filepath: data/song_data, in a pandas dataframe in order to then insert this record in *songs* and *artists* tables.
    3. process_log_file, which aims to load all the records of the files coming from filepath: data/log_data, in a pandas dataframe in order to then insert this record in *songplays*, *time* and *users* tables.
    4. main, which call inside is body: process_data twice (1st time for loading song data, and 2nd time for loading log data) and finally close the connction to the DB.

The relation with these .py files is described as follow:

1. **sql_queries.py** is imported in **create_tables.py** (importing only create_table_queries, drop_table_queries), in order to make the DDL queries (DROP and CREATE) available for the table creation.
2. **sql_queries.py** is imported in **etl.py** in order to perform DML queries (INSERT or SELECT) to write into the the tables previously created or performing a select statement.

## Istruction for running Python files:
In order to create and store data in the DB, the .py files need to be run in the following order:

1. **sql_queries.py** need to be run as first, it sets all the DROP IF EXISTS, CREATE, INSERT and SELECT statements, so these queries can be used in other files.
2. **create_tables.py** need to be run as second file. It sets the connection to the DB and runs DROP and CREATE statement imported from *sql_queries.py*.
3. **etl.py** need to be run as last file. It imports the INSERT statement from *sql_queries.py* in order to process and then insert values into the tables previously created.

## Example queries and results for song play analysis:

1. Query to retrieve users info combined to the songs and artists they are listening to.
    1. **QUERY:** SELECT sp.ts as datetime,u.first_name as "user name",u.last_name as "user last name",u.gender as "user gender", sp.level as "user level subscription", sp.location as "user location", sp.user_agent as "user agent", s.title as "song title", s.duration as "song duration", a.name as "artist name", a.location as " artist location" FROM songplays as sp JOIN songs as s ON sp.song_id = s.song_id JOIN artists as a ON a.artist_id = s.artist_id JOIN (SELECT user_id, max(first_name) as first_name, max(last_name) as last_name, max(gender) as gender FROM users group by user_id) as u ON u.user_id = sp.user_id;
    2. **OUTPUT:** {datetime:"2018-11-21 21:56:47.796000","user name":"Lily","user last name":"Koch","user gender":"F","user level subscription":"paid","user location":"Chicago-Naperville-Elgin, IL-IN-WI","user agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36","song title":"Setanta matins","song duration":269.58322,"artist name":"Elena","artist location":"Dubai UAE"}
2. Query to see how many paid subscriptions vs free subscriptions are in songplays.
    1. **QUERY:** SELECT level, count(level) as level_count, round(count(level) * 100.0 / (select count(*) from songplays),2) as level_percent_on_tot FROM songplays group by level ORDER BY 2 DESC;
    2. **OUTPUT:** {{"level":"paid","level_count":5591,"level_percent_on_tot":81.98}, {"level":"free","level_count":1229,"level_percent_on_tot":18.02}}
3. TOP 5 user location.
    1. **QUERY:** SELECT location as "user location", count(*) FROM songplays group by location ORDER BY 2 DESC LIMIT 5;
    2. **OUTPUT:** {{"San Francisco-Oakland-Hayward, CA":691}, {"Portland-South Portland, ME":665}, {"Lansing-East Lansing, MI":557}, {"Chicago-Naperville-Elgin, IL-IN-WI":475}, {"Atlanta-Sandy Springs-Roswell, GA":456}}
4. TOP 5 user agent.
    1. **QUERY:** SELECT user_agent, count(*) FROM songplays group by user_agent ORDER BY 2 DESC LIMIT 5;
    2. **OUTPUT:** {{"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36":971},{"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2":708},{"Mozilla/5.0 (Windows NT 5.1; rv:31.0)Gecko/20100101 Firefox/31.0":696},{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36":577}
