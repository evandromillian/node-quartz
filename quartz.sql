create schema quartz;
use quartz;

create table key_values(
	id int auto_increment,
    ky varchar(100),
    val varchar(2048),
    dt timestamp,
    primary key (id)
);

