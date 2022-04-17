create schema rental_schema;

drop table if exists rental_schema.sources CASCADE;
create table rental_schema.sources (
    id                          serial primary key,
    name                        varchar(32)
);

drop table if exists rental_schema.types CASCADE;
create table rental_schema.types (
    id                          serial primary key,
    name                        varchar(32)
);

drop table if exists rental_schema.pcd_age_bins CASCADE;
create table rental_schema.pcd_age_bins (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.pcd_household_types CASCADE;
create table rental_schema.pcd_household_types (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.pcd_dwelling_ownership CASCADE;
create table rental_schema.pcd_dwelling_ownership (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.pcd_resident_types CASCADE;
create table rental_schema.pcd_resident_types (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.pcd_dwelling_types CASCADE;
create table rental_schema.pcd_dwelling_types (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.pcd_economic_activity CASCADE;
create table rental_schema.pcd_economic_activity (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.pcd_education_level CASCADE;
create table rental_schema.pcd_education_level (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.pcd_health_status CASCADE;
create table rental_schema.pcd_health_status (
    id                          serial primary key,
    name                        varchar(64)
);

drop table if exists rental_schema.postcodes CASCADE;
create table rental_schema.postcodes (
    id                          serial primary key,
    postcode                    varchar(8),
    latitude                    float,
    longitude                   float,
    population_density          float,
    pt_accessibility            float,
    mean_income                 float
);

drop table if exists rental_schema.pcd_age_bin_share CASCADE;
create table rental_schema.pcd_age_bin_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    age_bin_id                  int not null references rental_schema.pcd_age_bins("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.pcd_household_type_share CASCADE;
create table rental_schema.pcd_household_type_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    household_type_id           int not null references rental_schema.pcd_household_types("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.pcd_dwelling_ownership_share CASCADE;
create table rental_schema.pcd_dwelling_ownership_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    dwelling_ownership_id       int not null references rental_schema.pcd_dwelling_ownership("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.pcd_resident_type_share CASCADE;
create table rental_schema.pcd_resident_type_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    resident_type_id            int not null references rental_schema.pcd_resident_types("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.pcd_dwelling_type_share CASCADE;
create table rental_schema.pcd_dwelling_type_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    dwelling_type_id            int not null references rental_schema.pcd_dwelling_types("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.pcd_economic_activity_share CASCADE;
create table rental_schema.pcd_economic_activity_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    economic_activity_id        int not null references rental_schema.pcd_economic_activity("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.pcd_education_level_share CASCADE;
create table rental_schema.pcd_education_level_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    education_level_id          int not null references rental_schema.pcd_education_level("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.pcd_health_status_share CASCADE;
create table rental_schema.pcd_health_status_share (
    id                          serial primary key,
    share                       float,
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade,
    health_status_id            int not null references rental_schema.pcd_health_status("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.address CASCADE;
create table rental_schema.address (
    id                          serial primary key,
    full_address                varchar(256),
    postcode_id                 int not null references rental_schema.postcodes("id")  on delete cascade on update cascade
);

drop table if exists rental_schema.listings CASCADE;
create table rental_schema.listings (
    id                          serial primary key,
    bedrooms                    int,
    bathrooms                   int,
    nearest_station_distance    float,
    price                       float,
    size_imputed                int,
    type_id                     int not null references rental_schema.types("id")  on delete cascade on update cascade,
    address_id                  int not null references rental_schema.address("id")  on delete cascade on update cascade,
    source_id                   int not null references rental_schema.sources("id")  on delete cascade on update cascade
);