drop table if exists update_time;
create table if not exists update_time (
    id int primary key auto_increment,
    app_name varchar(255),
    name varchar(255),
    last_update_time timestamp
);