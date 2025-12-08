# the purpose of this database in context of the startup, Sparkify, and their analytical goals.

the data base consist of two tables:
### log_events_table
Its log event data of the users interaction with the app songs stored in json format 
### songs_table
table for all songs in th app stored in json format, each song in file which will consume high amount of time to process.

### the use of these tables
We will compine these two tables so we could see how users interact with songs and which one do they prefer to use the most.

# database schema design and ETL pipeline.

the database consist of  dimentions  tables and one fact table

Fact Table \
1- songplays - records in event data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables\
2- users - users in the app
- user_id, first_name, last_name, gender, level
3- songs - songs in music database
- song_id, title, artist_id, year, duration
4- artists - artists in music database
- artist_id, name, location, latitude, longitude
5- time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

this design will enable us have the least number of `JOIN` with the availability of related data in the same Dimension so i can use the data without any `JOIN`.

# analysis queries
- how many listener in each hour?
`select t.hour, count(s.songplay_id)
from songplays s
JOIN time t 
ON s.start_time = t.start_time 
group by 1
order by 2 DESC;`

- Who are the most famous artists?
`SELECT a.artist, count(s.songplay_id)  
FROM songplays s
JOIN artists a
ON s.artist_id = a.artist_id
GROUP BY 1
order by 2 DESC
limit 10;`
- how many artists with no audiance?
`SELECT COUNT(artist)
FROM (SELECT a.artist, count(s.songplay_id) as num_of_listeners
    FROM songplays s  <br/>
    RIGHT JOIN artists a
    ON s.artist_id = a.artist_id
    GROUP BY 1)
where num_of_listeners=0;`