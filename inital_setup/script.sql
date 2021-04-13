create table states
(
    state_id  numeric primary key,
    name      varchar(256) not null,
    longitude float        not null,
    latitude  float        not null
);

create unique index state_idx
    on states (state_id);

create table info_per_day
(
    info_per_day_id     numeric primary key,
    state_id            numeric not null,
    confirmed           numeric,
    deaths              numeric,
    recovered           numeric,
    active              numeric,
    people_tested       numeric,
    people_hospitalized numeric,
    date                date,
    foreign key (state_id) references states (state_id)
);