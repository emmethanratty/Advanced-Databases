

/*Create ETL dimensional model*/
drop table fact_results;
drop table PlayerDim;
drop table TeamDim;
drop table TournamentDim;
drop table DateDim;


create table PlayerDim(
    player_sk integer primary key,
    player_name varchar(255)
);

create table TeamDim(
    team_sk integer primary key,
    team_name varchar(255)
);

create table TournamentDim(
    tournament_sk integer primary key,
    tournament_desc varchar(255),
    total_price float 
);

create table DateDim(
    date_sk integer primary key,
    day integer,
    month integer,
    year integer,
    week integer,
    quarter integer,
    dayOfWeek integer
);

create table fact_results(
    player_sk integer,
    tournament_sk integer,
    team_sk integer,
    date_sk integer,
    rank integer,
    price float,
    CONSTRAINT fk_dimDate FOREIGN KEY(date_sk) REFERENCES DateDim,
    CONSTRAINT fk_dimTournament FOREIGN KEY(tournament_sk) REFERENCES TournamentDim,
    CONSTRAINT fk_dimPlayer FOREIGN KEY(player_sk) REFERENCES PlayerDim,
    CONSTRAINT fk_dimTeam FOREIGN KEY(team_sk) REFERENCES TeamDim,
    CONSTRAINT pk_factresults PRIMARY KEY (date_sk, tournament_sk, player_sk, team_sk)
);

/*create stage for team */
drop table team_stage;
create table team_stage(
    sourceDB integer,
    team_sk integer,
    team_id integer,
    team_name varchar(255)
);

drop sequence team_stage_seq;
create sequence team_stage_seq
start with 1
increment by 1
nomaxvalue;

drop trigger team_stage_trigger;
create trigger team_stage_trigger
before insert on team_stage
for each row
begin
    select team_stage_seq.nextval into :new.team_sk from dual;
end;

insert into team_stage (sourcedb, team_id, team_name) 
select 1, team_id, team_name from Team1;

insert into team_stage (sourcedb, team_id, team_name) 
select 2, team_id, team_name from Team2;

/*Load dimension team */
insert into TeamDim select team_sk, team_name from team_stage;

/*create stage for player */
drop table p_stage;
create table p_stage(
    sourceDB integer,
    p_sk integer,
    p_id integer,
    p_name varchar(50),
    p_sname varchar(50),
    team_id integer
);

drop sequence p_stage_seq;
create sequence p_stage_seq
start with 1
increment by 1
nomaxvalue;

drop trigger p_stage_trigger;
create trigger p_stage_trigger
before insert on p_stage
for each row
begin
    select p_stage_seq.nextval into :new.p_sk from dual;
end;

insert into p_stage (sourcedb, p_id, p_name, p_sname, team_id) 
select 1, p_id, p_name, p_sname, team_id from Players1;

insert into p_stage (sourcedb, p_id, p_name, p_sname, team_id) 
select 2, p_id, p_name, p_sname, team_id from Players2;

/*Load dimension player */
insert into PlayerDim
select p_stage.p_sk,  p_stage.p_name || ' ' || p_stage.p_sname from p_stage where p_stage.sourceDB = 1;

insert into PlayerDim
select p_stage.p_sk, p_stage.p_name || ' ' || p_stage.p_sname
from p_stage 
where not exists (
  select player_name
  from PlayerDim
  where PlayerDim.player_name = p_stage.p_name || ' ' || p_stage.p_sname
) and sourcedb = 2;


/*create stage for tournament */
drop table t_stage;
create table t_stage(
    sourceDB integer,
    t_sk integer,
    t_id integer,
    t_descriprion varchar(100),
    t_date date,
    total_price float
);

drop sequence t_stage_seq;
create sequence t_stage_seq
start with 1
increment by 1
nomaxvalue;

drop trigger t_stage_trigger;
create trigger t_stage_trigger
before insert on t_stage
for each row
begin
    select t_stage_seq.nextval into :new.t_sk from dual;
end;

insert into t_stage (sourcedb, t_id, t_descriprion, t_date, total_price) 
select 1, t_id, t_descriprion, t_date, total_price from Tournament1;

insert into t_stage (sourcedb, t_id, t_descriprion, t_date, total_price) 
select 2, t_id, t_descriprion, t_date, total_price from Tournament2;

update t_stage
set total_price = total_price * 0.7
where sourcedb = 2;

/*Load dimension tournament */
insert into TournamentDim select t_sk, t_descriprion, total_price from t_stage;


/*date stage creation*/
drop table d_stage;
create table d_stage(
    d_sk integer,
    day integer,
    month integer,
    year integer,
    week integer,
    quarter integer,
    dayOfWeek varchar(20),
    t_date date
);

drop sequence d_stage_seq;
create sequence d_stage_seq
start with 1
increment by 1
nomaxvalue;

drop trigger d_stage_trigger;
create trigger d_stage_trigger
before insert on d_stage
for each row
begin
    select d_stage_seq.nextval into :new.d_sk from dual;
end;

insert into d_stage( day, year, month, week, quarter, dayOfWeek,t_date) 
select  
cast(to_char(t_date,'DD') as integer),
cast(to_char(t_date,'YYYY') as integer), 
cast(to_char(t_date,'MM') as integer),
to_number(to_char(to_date(t_date,'DD/MM/YYYY'),'WW')),
to_number(to_char(to_date(t_date,'DD/MM/YYYY'),'Q')),
to_number(to_char(to_date(t_date,'DD/MM/YYYY'),'D')),
t_date from tournament1;

insert into d_stage( day, year, month, week, quarter, dayOfWeek,t_date) 
select  
cast(to_char(t_date,'DD') as integer),
cast(to_char(t_date,'YYYY') as integer), 
cast(to_char(t_date,'MM') as integer),
to_number(to_char(to_date(t_date,'DD/MM/YYYY'),'WW')),
to_number(to_char(to_date(t_date,'DD/MM/YYYY'),'Q')),
to_number(to_char(to_date(t_date,'DD/MM/YYYY'),'D')),
t_date from tournament2;

insert into DateDim select d_sk, day, month, year, week, quarter, dayOfWeek from d_stage;


/*creating fact table*/
drop table facts_stage;
create table facts_stage(
    p_sk integer,
    t_sk integer,
    team_sk integer,
    d_sk integer,
    p_id integer,
    team_id integer,
    t_id integer,
    rank integer,
    price float,
    sourcedb integer,
    full_date date
);
/*  */


insert into facts_stage(t_id, p_id, team_id, full_date, rank, price, sourcedb) 
select r.t_id, r.p_id, p.team_id, t.t_date, r.rank, r.price, 1 from Results1 r, Players1 p , Tournament1 t
where r.p_id = p.p_id and r.t_id = t.t_id;

insert into facts_stage(t_id, p_id, team_id, full_date, rank, price, sourcedb) 
select r.t_id, r.p_id, p.team_id, t.t_date, r.rank, r.price, 2 from Results2 r, Players2 p , Tournament2 t
where r.p_id = p.p_id and r.t_id = t.t_id;


update facts_stage
set team_sk=
	(select team_stage.team_sk from
	team_stage where (team_stage.sourceDB=facts_stage.sourceDB and
	team_stage.team_id = facts_stage.team_id));
	
update facts_stage
set p_sk=
	(select p_stage.p_sk from
	p_stage where (p_stage.sourceDB=facts_stage.sourceDB and
	p_stage.p_id = facts_stage.p_id));
	
/*Tournament SK*/
update facts_stage
set t_sk =
	(select t_stage.t_sk from
	t_stage where (t_stage.sourceDB=facts_stage.sourceDB and
	t_stage.t_id = facts_stage.t_id));
	
update facts_stage
set d_sk=
	(select d_stage.d_sk from
	d_stage where (d_stage.t_date=facts_stage.full_date));
  

update facts_stage
set price = price * 0.7 
where sourcedb = 2;

update facts_stage
set t_sk =1
where t_sk = 7 and sourceDB = 2;

update facts_stage
set p_sk = 1
where p_sk = 7 and sourceDB = 2;

update facts_stage
set p_sk = 5
where p_sk = 8 and sourceDB = 2;

insert into fact_results(player_sk, tournament_sk, team_sk, date_sk, rank, price) 
select p_sk, t_sk, team_sk, d_sk, rank, price from facts_stage;


INSERT INTO PLAYERS1 (P_ID, P_NAME, P_SNAME, TEAM_ID) VALUES (7, 'Alan', 'Parker', 1);
INSERT INTO PLAYERS1 (P_ID, P_NAME, P_SNAME, TEAM_ID) VALUES (8, 'Martha', 'Bag', 2);
INSERT INTO TOURNAMENT1 (T_ID, T_DESCRIPRION, t_date, TOTAL_PRICE) VALUES (5, 'Saudi Open', '22-Nov-14', 500000);
INSERT INTO RESULTS1 (T_ID, P_ID, RANK, PRICE) VALUES (5, 1, 1, 60000);
INSERT INTO RESULTS1 (T_ID, P_ID, RANK, PRICE) VALUES (5, 7, 5, 20000);
INSERT INTO RESULTS1 (T_ID, P_ID, RANK, PRICE) VALUES (2, 8, 3, 1000);

insert into p_stage ( sourcedb, p_id, p_name, p_sname, team_id)
select 1, p_id, p_name, p_sname, team_id from players1
where not exists( select * from p_stage p where p.p_id = players1.p_id and sourcedb = 1 );

insert into PlayerDim
select p_stage.p_sk,  p_stage.p_name || ' ' || p_stage.p_sname from p_stage 
where not exists(select * from PlayerDim where PlayerDim.player_sk = p_stage.p_sk) and sourcedb = 1;

insert into t_stage (sourcedb, t_id, t_descriprion, total_price)
select 1, t_id, t_descriprion, total_price from TOURNAMENT1 
where not exists ( select * from t_stage t where t.t_id = tournament1.t_id and sourcedb = 1 );

insert into TournamentDim 
select ts.t_sk, ts.t_descriprion, ts.total_price from t_stage ts
where not exists (select * from TournamentDim t where t.tournament_sk = ts.t_sk  ) and sourcedb = 1;


insert into facts_stage(t_id, p_id, team_id, full_date, rank, price, sourcedb) 
select r.t_id, r.p_id, p.team_id, t.t_date, r.rank, r.price, 1 from Results1 r, Players1 p , Tournament1 t
where r.p_id = p.p_id and r.t_id = t.t_id and 
NOT EXISTS (SELECT * FROM facts_stage fs
              WHERE fs.t_id = r.t_id and fs.p_id = r.p_id and fs.team_id = p.team_id and fs.full_date = t.t_date and fs.rank =r.rank and fs.price = r.price
			  and fs.sourcedb = 1 )

update facts_stage
set team_sk=
	(select team_stage.team_sk from
	team_stage where (team_stage.sourceDB=facts_stage.sourceDB and
	team_stage.team_id = facts_stage.team_id));
	
update facts_stage
set p_sk=
	(select p_stage.p_sk from
	p_stage where (p_stage.sourceDB=facts_stage.sourceDB and
	p_stage.p_id = facts_stage.p_id));
	
/*Tournament SK*/
update facts_stage
set t_sk =
	(select t_stage.t_sk from
	t_stage where (t_stage.sourceDB=facts_stage.sourceDB and
	t_stage.t_id = facts_stage.t_id));
	
update facts_stage
set d_sk=
	(select d_stage.d_sk from
	d_stage where (d_stage.t_date=facts_stage.full_date));

insert into fact_results(player_sk, tournament_sk, team_sk, date_sk, rank, price) 
select p_sk, t_sk, team_sk, d_sk, rank, price from facts_stage fs where 
not exists(select * from fact_results fr where fr.player_sk = fs.p_sk 
and fr.tournament_sk = fs.t_sk and fr.team_sk = fs.t_sk and fr.date_sk = fs.d_sk ) and sourcedb = 1;