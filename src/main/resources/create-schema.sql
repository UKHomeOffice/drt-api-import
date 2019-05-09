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

CREATE INDEX voyage_manifest_passenger_info_arrival_dow_woy ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, departure_port_code, voyage_number, week_of_year, day_of_week);
CREATE INDEX voyage_manifest_passenger_info_arrival_woy ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, departure_port_code, voyage_number, week_of_year);
CREATE INDEX voyage_manifest_passenger_info_route_woy_dow ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, departure_port_code, week_of_year, day_of_week);
CREATE INDEX voyage_manifest_passenger_info_egate_eligibility ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, document_type, nationality_country_code, age);
CREATE INDEX voyage_manifest_passenger_info_egate_eligibility_with_scheduled ON public.voyage_manifest_passenger_info (event_code, arrival_port_code, document_type, nationality_country_code, age);
CREATE INDEX voyage_manifest_passenger_info_unique_arrival ON public.voyage_manifest_passenger_info (arrival_port_code, departure_port_code, scheduled_date, voyage_number);
CREATE INDEX voyage_manifest_passenger_info_json_file ON public.voyage_manifest_passenger_info (json_file);

CREATE TABLE public.processed_json (
    zip_file_name character varying (50),
    json_file_name character varying (50),
    suspicious_date boolean,
    success boolean,
    processed_at timestamp without time zone
);

CREATE INDEX processed_json_zip_file_name ON public.processed_json (zip_file_name);
CREATE INDEX processed_json_json_file_name ON public.processed_json (json_file_name);
CREATE INDEX processed_json_suspicious_date ON public.processed_json (suspicious_date);
CREATE INDEX processed_json_succcess ON public.processed_json (success);


CREATE TABLE public.processed_zip (
    zip_file_name character varying (50),
    success boolean,
    processed_at timestamp without time zone
);

CREATE INDEX processed_zip_zip_file_name ON public.processed_zip (zip_file_name);
CREATE INDEX processed_zip_succcess ON public.processed_zip (success);
