create table book (
    asin varchar not null,
    isbn varchar,
    answered_questions int,
    availability varchar,
    brand varchar,
    currency varchar,
    date_first_available date,
    delivery varchar[],
    description varchar,
    discount decimal,
    domain varchar,
    features varchar[],
    final_price decimal,
    formats jsonb,
    image_url varchar,
    images_count int,
    initial_price decimal,
    item_weight varchar,
    manufacturer varchar,
    model_number varchar,
    plus_content boolean,
    product_dimensions varchar,
    rating varchar,
    reviews_count int,
    root_bs_rank int,
    seller_id varchar,
    seller_name varchar,
    timestamp timestamptz,
    title varchar,
    url varchar,
    video boolean,
    video_count int,
    categories varchar[],
    best_sellers_rank jsonb,
    primary key (asin)
);

create table error_state (
    id serial,
    domain varchar,
    reference varchar,
    violations jsonb,
    primary key (id)
);

-- drop table ingestion_log_entry_status:
create table ingestion_log_entry_status (
    id int,
    name varchar,
    primary key (id)
);

insert into ingestion_log_entry_status values
    (10, 'UPLOAD_STARTED'),
    (11, 'UPLOAD_FAILED'),
    (12, 'UPLOAD_COMPLETED'),
    (13, 'PROCESSING_STARTED'),
    (21, 'PROCESSING_FAILED'),
    (22, 'PROCESSING_SUCCEEDED'),
    (30, 'MERGE_STARTED'),
    (31, 'MERGE_FAILED'),
    (32, 'MERGE_SUCCEEDED');

-- DROP table ingestion_log_entry;
create table ingestion_log_entry (
    id serial,
    path varchar not null,
    create_time timestamptz not null default now(),
    status_id int not null,
    primary key (id),
    foreign key (status_id) references ingestion_log_entry_status (id)
);

-- drop table book_ingestion
create table book_ingestion (
    ingestion_log_entry_id int,
    asin varchar not null,
    isbn varchar,
    answered_questions int,
    availability varchar,
    brand varchar,
    currency varchar,
    date_first_available date,
    delivery varchar[],
    description varchar,
    discount decimal,
    domain varchar,
    features varchar[],
    final_price decimal,
    formats jsonb,
    image_url varchar,
    images_count int,
    initial_price decimal,
    item_weight varchar,
    manufacturer varchar,
    model_number varchar,
    plus_content boolean,
    product_dimensions varchar,
    rating varchar,
    reviews_count int,
    root_bs_rank int,
    seller_id varchar,
    seller_name varchar,
    timestamp timestamptz,
    title varchar,
    url varchar,
    video boolean,
    video_count int,
    categories varchar[],
    best_sellers_rank jsonb,
    primary key (ingestion_log_entry_id, asin),
    foreign key (ingestion_log_entry_id) references ingestion_log_entry (id)
);