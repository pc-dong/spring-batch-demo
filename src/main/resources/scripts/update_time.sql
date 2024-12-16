create table  if not exists update_time (
    id int primary key auto_increment,
    name varchar(255),
    last_update_time timestamp
);