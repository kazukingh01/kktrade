CREATE TABLE public.economic_calendar (
    id integer NOT NULL,
    unixtime bigint NOT NULL,
    name text,
    country text,
    importance smallint NOT NULL,
    actual real,
    previous real,
    consensus real,
    forecast real,
    unit text,
    unit2 text
);

ALTER TABLE public.economic_calendar OWNER TO postgres;


ALTER TABLE ONLY public.economic_calendar
    ADD CONSTRAINT economic_calendar_pkey PRIMARY KEY (id, unixtime);

CREATE INDEX economic_calendar_0 ON public.economic_calendar USING btree (id);
CREATE INDEX economic_calendar_1 ON public.economic_calendar USING btree (unixtime);
