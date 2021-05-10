/* To use: PGPASSWORD={password} psql --host={host} --port 5439 --user={user} --dbname={db} -f protocol_perf_test.sql */
/* All used tables have 5 columns, and all columns within the same table have the same datatype */

drop table if exists perf_varchar;
create table perf_varchar (val1 varchar, val2 varchar, val3 varchar, val4 varchar, val5 varchar);

drop table if exists perf_time;
create table perf_time (val1 time, val2 time, val3 time, val4 time, val5 time);

drop table if exists perf_timetz;
create table perf_timetz (val1 timetz, val2 timetz, val3 timetz, val4 timetz, val5 timetz);

drop table if exists perf_timestamptz;
create table perf_timestamptz (val1 timestamptz, val2 timestamptz, val3 timestamptz, val4 timestamptz, val5 timestamptz);

insert into perf_varchar values('abcd¬µ3kt¿abcdÆgda123~Øasd', 'abcd¬µ3kt¿abcdÆgda123~Øasd', 'abcd¬µ3kt¿abcdÆgda123~Øasd', 'abcd¬µ3kt¿abcdÆgda123~Øasd', 'abcd¬µ3kt¿abcdÆgda123~Øasd');
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);
insert into perf_varchar (select * from perf_varchar);

insert into perf_time values('12:13:14', '12:13:14', '12:13:14', '12:13:14', '12:13:14');
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);
insert into perf_time (select * from perf_time);

insert into perf_timetz values('20:13:14.123456', '20:13:14.123456', '20:13:14.123456', '20:13:14.123456', '20:13:14.123456');
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);
insert into perf_timetz (select * from perf_timetz);

insert into perf_timestamptz values('1997-10-11 07:37:16', '1997-10-11 07:37:16', '1997-10-11 07:37:16', '1997-10-11 07:37:16', '1997-10-11 07:37:16');
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
insert into perf_timestamptz (select * from perf_timestamptz);
