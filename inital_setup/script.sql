create table states
(
    state_id  integer primary key,
    name      varchar(256) not null,
    longitude float        not null,
    latitude  float        not null
);

create unique index state_idx
    on states (state_id);

create table info_per_day
(
    info_per_day_id     integer primary key,
    state_id            integer not null,
    confirmed           integer,
    deaths              integer,
    recovered           integer,
    active              integer,
    people_tested       integer,
    people_hospitalized integer,
    date                date,
    foreign key (state_id) references states (state_id)
);