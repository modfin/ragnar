CREATE TABLE IF NOT EXISTS public.access_token
(
    access_key_id     text                     default encode(gen_random_bytes(10), 'hex') PRIMARY KEY,
    access_key        text                     default 'rag_' || gen_random_uuid() NOT NULL UNIQUE,

    token_name        text                     default ''                          NOT NULL,
    allow_create_tubs boolean                  default false                       NOT NULL,
    allow_read_tubs   boolean                  default false                       NOT NULL,

    created_at        timestamp with time zone default now()                       NOT NULL,
    updated_at        timestamp with time zone default now()                       NOT NULL,
    deleted_at        timestamp with time zone
);


CREATE TABLE IF NOT EXISTS public.tub
(
    tub_id     text                     DEFAULT ('tub_' || gen_random_uuid()) PRIMARY KEY,
    tub_name   text                                   NOT NULL UNIQUE,

    settings   hstore                   default '',

    created_at timestamp with time zone default now() NOT NULL,
    updated_at timestamp with time zone default now() NOT NULL,
    deleted_at timestamp with time zone
);


CREATE TABLE IF NOT EXISTS public.tub_acl
(
    access_key_id text                                   NOT NULL references public.access_token (access_key_id) on delete cascade,
    tub_id        text                                   NOT NULL references public.tub (tub_id) on delete cascade,
    tub_name      text                                   NOT NULL references public.tub (tub_name) on delete cascade,


    -- CRUD ops
    allow_create  boolean                  default false NOT NULL,
    allow_read    boolean                  default false NOT NULL,
    allow_update  boolean                  default false NOT NULL,
    allow_delete  boolean                  default false NOT NULL,

    created_at    timestamp with time zone default now() NOT NULL,
    deleted_at    timestamp with time zone,

    PRIMARY KEY (access_key_id, tub_id)
);

