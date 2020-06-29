class SqlQueries:
    create_staging_events = ("""
        CREATE TABLE IF NOT EXISTS public.events_stage (
        index integer,
        concert varchar(256),
        artist varchar(256),
        location varchar(256),
        start_date date
        );
    """)
                          
    create_staging_songs = ("""
        CREATE TABLE IF NOT EXISTS public.songs_stage (
        index integer,
        artist varchar(256),
        artist_listeners integer,
        song varchar(256),
        song_playcount integer,
        song_listeners integer
        );
    """)

    create_dwh_songs = ("""
        CREATE TABLE IF NOT EXISTS public.songs_dwh (
        song varchar(256),
        song_playcount integer,
        song_listeners integer,
        artist varchar(256)
        );
    """)

    create_dwh_artists = ("""
        CREATE TABLE IF NOT EXISTS public.artists_dwh (
        artists varchar(256),
        artist_listeners integer
        );
    """)

    create_dwh_concerts = ("""
        CREATE TABLE IF NOT EXISTS public.concerts_dwh (
        name varchar(256),
        location varchar(256),
        start_date date,
        artist varchar(256)
        );
    """)


    songs_insert = ("""
            INSERT INTO public.songs_dwh (song, song_playcount, song_listeners, artist)
            SELECT song, song_playcount, song_listeners, artist
            FROM public.songs_stage  
            WHERE  artist IS NOT NULL          
    """)

    artists_insert = ("""
            INSERT INTO public.artists_dwh (artists, artist_listeners)
            SELECT DISTINCT ss.artist, ss.artist_listeners
            FROM public.songs_stage ss
            FULL JOIN public.events_stage es  
            ON ss.artist = es.artist  
            WHERE  ss.artist IS NOT NULL  AND es.artist IS NOT NULL     
    """)

    concerts_insert = ("""
            INSERT INTO public.concerts_dwh (name, location, start_date, artist)
            SELECT concert, location, start_date, artist
            FROM public.events_stage  
            WHERE  artist IS NOT NULL             
    """)


    delete_staging_events = "DROP TABLE IF EXISTS public.events_stage;"
    delete_staging_songs = "DROP TABLE IF EXISTS public.songs_stage;"

    delete_dwh_songs = "DROP TABLE IF EXISTS public.songs_dwh;"
    delete_dwh_artists = "DROP TABLE IF EXISTS public.artists_dwh;"
    delete_dwh_concerts = "DROP TABLE IF EXISTS public.concerts_dwh;"

    create_staging_table_queries = [create_staging_events, create_staging_songs]
    delete_staging_table_queries = [delete_staging_events, delete_staging_songs]

    create_dwh_table_queries = [create_dwh_songs, create_dwh_artists, create_dwh_concerts]
    delete_dwh_table_queries = [delete_dwh_songs, delete_dwh_artists, delete_dwh_concerts]