## Description
---
This repo provides the ETL pipeline, to populate the postgresetl database.  
* The purpose of this database is to enable User to answer business questions it may have of its users, the types of songs they listen to and the artists of those songs using the data that it has in logs and files. The database provides a consistent and reliable source to store this data.

* This source of data will be useful in helping the user achieve some of its analytical goals, such as identifying the most popular songs or determining peak traffic times of the day.

## Database Design and ETL Pipeline
---
* For the schema design, the STAR schema is used as it simplifies queries and provides fast aggregations of data.

![Schema](schema.PNG)

* For the ETL pipeline, Python is used as it contains libraries such as pandas, that simplifies data manipulation. It also allows connection to Postgres Database.

* There are 2 types of data involved, song and log data. For song data, it contains information about songs and artists, which we extract from and load into users and artists dimension tables

* Log data gives the information of each user session. From log data, we extract and load into time, users dimension tables and songplays fact table.

## Running the ETL Pipeline
---
* First, run createConnection.py to create the data tables using the schema design specified. If tables were created previously, they will be dropped and recreated.

* Next, run load.py to populate the data tables created.