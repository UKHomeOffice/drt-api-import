-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE public.voyage_manifest_passenger_info (
    event_code character varying(2),
    arrival_port_code character varying(5),
    departure_port_code character varying(5),
    voyage_number integer,
    carrier_code character varying(5),
    scheduled_date timestamp without time zone,
    day_of_week smallint,
    week_of_year smallint,
    document_type character varying(50),
    document_issuing_country_code character varying(5),
    eea_flag character varying(5),
    age smallint,
    disembarkation_port_code character varying(5),
    in_transit_flag character varying(2),
    disembarkation_port_country_code character varying(5),
    nationality_country_code character varying(5),
    passenger_identifier character varying(25),
    in_transit boolean,
    json_file character varying(50)
);

CREATE INDEX voyage_manifest_passenger_arrival_dow_woy ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, departure_port_code, voyage_number, week_of_year, day_of_week);
CREATE INDEX voyage_manifest_passenger_arrival_woy ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, departure_port_code, voyage_number, week_of_year);
CREATE INDEX voyage_manifest_passenger_route_woy_dow ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, departure_port_code, week_of_year, day_of_week);
CREATE INDEX voyage_manifest_passenger_egate_eligibility ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, document_type, nationality_country_code, age);
CREATE INDEX voyage_manifest_passenger_egate_eligibility_with_scheduled ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, document_type, nationality_country_code, age);
CREATE INDEX voyage_manifest_passenger_unique_arrival ON public.voyage_manifest_passenger_info (arrival_port_code, departure_port_code, scheduled_date, voyage_number);
CREATE INDEX voyage_manifest_passenger_json_file ON public.voyage_manifest_passenger_info (json_file);

CREATE TABLE public.processed_json (
    zip_file_name character varying (50),
    json_file_name character varying (50),
    suspicious_date boolean,
    success boolean,
    processed_at timestamp without time zone,
    arrival_port_code varchar(5),
    departure_port_code varchar(5),
    voyage_number integer,
    scheduled timestamp without time zone,
    event_code varchar(3),
    non_interactive_total_count smallint,
    non_interactive_trans_count smallint,
    interactive_total_count smallint,
    interactive_trans_count smallint
);

CREATE INDEX processed_json_zip_file_name ON public.processed_json (zip_file_name);
CREATE INDEX processed_json_json_file_name ON public.processed_json (json_file_name);
CREATE INDEX processed_json_suspicious_date ON public.processed_json (suspicious_date);
CREATE INDEX processed_json_success ON public.processed_json (success);
CREATE INDEX processed_json_unique_arrival ON public.processed_json (arrival_port_code, departure_port_code, scheduled, voyage_number, event_code);
CREATE INDEX processed_json_arrival_port_date_event_code ON public.processed_json (arrival_port_code, cast(scheduled as date), event_code);

CREATE TABLE public.processed_zip (
    zip_file_name character varying (50),
    success boolean,
    processed_at timestamp without time zone,
    created_on varchar(10)
);

CREATE INDEX processed_zip_zip_file_name ON public.processed_zip (zip_file_name);
CREATE INDEX processed_zip_success ON public.processed_zip (success);
CREATE INDEX processed_zip_processed_at ON public.processed_zip (processed_at);
CREATE INDEX processed_zip_created_on ON processed_zip (created_on);

/*update processed_zip set created_on = concat('20',substring(zip_file_name, 8, 2),'-',substring(zip_file_name, 10, 2),'-',substring(zip_file_name, 12, 2))::date
   where created_on is null;*/
