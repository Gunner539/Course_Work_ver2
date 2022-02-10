create_commands = [
    '''
        CREATE TABLE IF NOT EXISTS Clients(
            id integer primary key,
            c_name character varying(100) not null,
            c_surname character varying(100) not null,
            c_sex integer not null,
            c_upd timestamp);

    ''',
    '''
        CREATE TABLE IF NOT EXISTS Users(
            id integer primary key,
            u_name character varying(100) not null,
            u_surname character varying(100) not null,
            u_status integer,
            u_age integer,
            u_sex integer,
            u_upd timestamp);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS Clients_Users(
            id serial primary key,
            client_id integer references Clients(id) not null,
            user_id integer references Users(id) not null,
            liked boolean,
            banned boolean,
            upd timestamp,
            CONSTRAINT user_client_couple UNIQUE (client_id, user_id));
    ''',

    '''
        CREATE TABLE IF NOT EXISTS User_photos(
            photo_id character varying(100) primary key,
            user_id integer references Users(id) not null
            );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS Searches(
            id serial primary key,
            client_id integer references Clients(id) not null unique,
            city character varying(100),
            city_id integer,
            status_id integer,
            sex integer,
            min_age integer,
            max_age integer,
            upd timestamp);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS Search_users(
            u_id integer references Users(id) not null,
            search_id integer references Searches(id) not null,
            CONSTRAINT s_us UNIQUE (u_id, search_id));
    '''
]

delete_commands = [
    'drop table if exists Clients cascade',
    'drop table if exists Users cascade',
    'drop table if exists Clients_Users cascade',
    'drop table if exists Photos cascade',
    'drop table if exists User_photos cascade',
    'drop table if exists Searches cascade',
    'drop table if exists Search_Users cascade'
]
