DROP_ARTISTS_TABLE_SQL = """
DROP TABLE IF EXISTS public.dim_artists;
"""

CREATE_ARTISTS_TABLE_SQL = """
CREATE TABLE public.dim_artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(17,0),
	longitude numeric(17,0)
);
"""
DROP_SONGPLAYS_TABLE_SQL = """
DROP TABLE IF EXISTS public.songplays_fact;
"""

CREATE_SONGPLAYS_TABLE_SQL = """
CREATE TABLE public.songplays_fact (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
"""

DROP_SONGS_TABLE_SQL = """
DROP TABLE IF EXISTS public.dim_songs;
"""

CREATE_SONGS_TABLE_SQL = """
CREATE TABLE public.dim_songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
"""

DROP_STAGE_EVENTS_TABLE_SQL = """
DROP TABLE IF EXISTS public.stage_events;
"""

CREATE_STAGE_EVENTS_TABLE_SQL = """
CREATE TABLE public.stage_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);
"""

DROP_STAGE_SONGS_TABLE_SQL = """
DROP TABLE IF EXISTS public.stage_songs;
"""

CREATE_STAGE_SONGS_TABLE_SQL = """
CREATE TABLE public.stage_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);
"""

DROP_TIME_TABLE_SQL = """
DROP TABLE IF EXISTS public.dim_time;
"""

CREATE_TIME_TABLE_SQL = """
CREATE TABLE public.dim_time (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);
"""

DROP_USERS_TABLE_SQL = """
DROP TABLE IF EXISTS public.dim_users;
"""

CREATE_USERS_TABLE_SQL = """
CREATE TABLE public.dim_users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
"""

create_all_tables = [DROP_ARTISTS_TABLE_SQL,DROP_SONGPLAYS_TABLE_SQL,DROP_SONGS_TABLE_SQL,
                     DROP_STAGE_EVENTS_TABLE_SQL,DROP_STAGE_SONGS_TABLE_SQL,DROP_TIME_TABLE_SQL,
                     DROP_USERS_TABLE_SQL, CREATE_ARTISTS_TABLE_SQL, CREATE_SONGPLAYS_TABLE_SQL,
                     CREATE_SONGS_TABLE_SQL, CREATE_STAGE_EVENTS_TABLE_SQL,CREATE_STAGE_SONGS_TABLE_SQL,
                     CREATE_TIME_TABLE_SQL, CREATE_USERS_TABLE_SQL]