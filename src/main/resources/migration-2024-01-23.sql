-- noinspection SqlNoDataSourceInspectionForFile

alter table processed_json
    add column arrival_port_code varchar(5);
alter table processed_json
    add column departure_port_code varchar(5);
alter table processed_json
    add column voyage_number integer;
alter table processed_json
    add column carrier_code varchar(5);
alter table processed_json
    add column scheduled timestamp without time zone;
alter table processed_json
    add column event_code varchar(3);
alter table processed_json
    add column non_interactive_total_count smallint;
alter table processed_json
    add column non_interactive_trans_count smallint;
alter table processed_json
    add column interactive_total_count smallint;
alter table processed_json
    add column interactive_trans_count smallint;

CREATE INDEX processed_json_unique_arrival ON public.processed_json (arrival_port_code, departure_port_code, scheduled,
                                                                     voyage_number, event_code);
CREATE INDEX processed_json_route_day_week_year ON public.processed_json (arrival_port_code, departure_port_code,
                                                                          event_code, extract(week from scheduled),
                                                                          extract(year from scheduled));
CREATE INDEX processed_json_arrival_week_year ON public.processed_json (arrival_port_code, departure_port_code,
                                                                        voyage_number, event_code,
                                                                        extract(week from scheduled),
                                                                        extract(year from scheduled));
CREATE INDEX processed_json_arrival_day_week_year ON public.processed_json (arrival_port_code, departure_port_code,
                                                                            voyage_number, event_code,
                                                                            extract(dow from scheduled),
                                                                            extract(week from scheduled),
                                                                            extract(year from scheduled));
CREATE INDEX processed_json_arrival_port_date_event_code ON public.processed_json (arrival_port_code, cast(scheduled as date), event_code);

alter table processed_zip
    add created_on varchar(10);

create index processed_zip_created_on on processed_zip (created_on);

update processed_zip
set created_on = concat('20', substring(zip_file_name, 8, 2), '-', substring(zip_file_name, 10, 2), '-',
                        substring(zip_file_name, 12, 2))::date
where created_on is null;
