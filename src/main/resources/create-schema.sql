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
    processed_at timestamp without time zone
);

CREATE INDEX processed_json_zip_file_name ON public.processed_json (zip_file_name);
CREATE INDEX processed_json_json_file_name ON public.processed_json (json_file_name);
CREATE INDEX processed_json_suspicious_date ON public.processed_json (suspicious_date);
CREATE INDEX processed_json_success ON public.processed_json (success);


CREATE TABLE public.processed_zip (
    zip_file_name character varying (50),
    success boolean,
    processed_at timestamp without time zone
);

CREATE INDEX processed_zip_zip_file_name ON public.processed_zip (zip_file_name);
CREATE INDEX processed_zip_success ON public.processed_zip (success);
CREATE INDEX processed_zip_processed_at ON public.processed_zip (processed_at);

SELECT
  vm.arrival_port_code, vm.departure_port_code, vm.voyage_number, vm.scheduled_date, pz.processed_at
FROM processed_zip pz
INNER JOIN processed_json pj ON pz.zip_file_name=pj.zip_file_name
INNER JOIN voyage_manifest_passenger_info vm ON vm.json_file = pj.json_file_name
WHERE pz.processed_at>='2022-03-09T17:00' AND vm.arrival_port_code='LHR'
GROUP BY vm.arrival_port_code, vm.departure_port_code, vm.carrier_code, vm.voyage_number, vm.scheduled_date, pz.processed_at
ORDER BY pz.processed_at;

SELECT
    vm.departure_port_code, vm.voyage_number, vm.scheduled_date, pz.processed_at
FROM processed_zip pz
         INNER JOIN processed_json pj ON pz.zip_file_name = pj.zip_file_name
         INNER JOIN voyage_manifest_passenger_info vm ON vm.json_file = pj.json_file_name
WHERE
        pz.processed_at >= '2022-03-09T17:00'
  AND vm.arrival_port_code = ${destinationPortCode.iata}
  AND event_code = 'DC'
GROUP BY vm.departure_port_code, vm.voyage_number, vm.scheduled_date, pz.processed_at
ORDER BY pz.processed_at
