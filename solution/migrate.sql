/*CDM*/
create table cdm.user_product_counters
(
    id           integer generated always as identity,
    user_id      uuid not null,
    product_id   uuid not null,
    product_name varchar not null ,
    order_cnt    integer default 0 not null,
    primary key (id),
    unique (user_id, product_id),
    constraint user_product_counters_order_cnt_check
        check (order_cnt >= 0)
);
create table cdm.user_category_counters
(
    id           integer generated always as identity,
    user_id      uuid not null,
    category_id   uuid not null,
    category_name varchar not null ,
    order_cnt    integer default 0 not null,
    primary key (id),
    unique (user_id, category_id),
    constraint user_product_counters_order_cnt_check
        check (order_cnt >= 0)
);
/*STG*/
create table stg.order_events(
                                 id integer generated always as identity primary key,
                                 object_id integer not null unique,
                                 object_type varchar not null,
                                 sent_dttm timestamp not null,
                                 payload json not null
);

/*DDS*/
/*HUBS*/
create table dds.h_user(
                           h_user_pk uuid primary key,
                           user_id varchar not null,
                           load_dt timestamp not null,
                           load_src varchar not null
);
create table dds.h_product(
                              h_product_pk uuid primary key,
                              product_id varchar not null,
                              load_dt timestamp not null,
                              load_src varchar not null
);
create table dds.h_category(
                               h_category_pk uuid primary key,
                               category_name varchar not null,
                               load_dt timestamp not null,
                               load_src varchar not null
);
create table dds.h_restaurant(
                                 h_restaurant_pk uuid primary key,
                                 restaurant_id varchar not null,
                                 load_dt timestamp not null,
                                 load_src varchar not null
);
create table dds.h_order(
                            h_order_pk uuid primary key,
                            order_id integer not null,
                            order_dt timestamp not null,
                            load_dt timestamp not null,
                            load_src varchar not null
);
/*LINKS*/
create table dds.l_order_product(
                                    hk_order_product_pk uuid primary key,
                                    h_order_pk uuid not null,
                                    h_product_pk uuid not null,
                                    load_dt timestamp not null,
                                    load_src varchar not null,
                                    foreign key (h_order_pk) references dds.h_order(h_order_pk),
                                    foreign key (h_product_pk) references dds.h_product(h_product_pk)
);
create table dds.l_product_restaurant(
                                         hk_product_restaurant_pk uuid primary key,
                                         h_product_pk uuid not null,
                                         h_restaurant_pk uuid not null,
                                         load_dt timestamp not null,
                                         load_src varchar not null,
                                         foreign key (h_restaurant_pk) references dds.h_restaurant(h_restaurant_pk),
                                         foreign key (h_product_pk) references dds.h_product(h_product_pk)
);
create table dds.l_product_category(
                                       hk_product_category_pk uuid primary key,
                                       h_product_pk uuid not null,
                                       h_category_pk uuid not null,
                                       load_dt timestamp not null,
                                       load_src varchar not null,
                                       foreign key (h_category_pk) references dds.h_category(h_category_pk),
                                       foreign key (h_product_pk) references dds.h_product(h_product_pk)
);
create table dds.l_order_user(
                                 hk_order_user_pk uuid primary key,
                                 h_order_pk uuid not null,
                                 h_user_pk uuid not null,
                                 load_dt timestamp not null,
                                 load_src varchar not null,
                                 foreign key (h_order_pk) references dds.h_order(h_order_pk),
                                 foreign key (h_user_pk) references dds.h_user(h_user_pk)
);
/*Satellites*/
create table if not exists dds.s_user_names(
                                               h_user_pk uuid references dds.h_user(h_user_pk),
                                               username varchar not null ,
                                               userlogin varchar not null ,
                                               load_dt timestamp not null ,
                                               load_src varchar not null ,
                                               hk_user_names_hashdiff uuid not null ,
                                               primary key (h_user_pk, load_dt)
);

create table if not exists dds.s_product_names(
                                                  h_product_pk uuid references dds.h_product(h_product_pk),
                                                  name varchar not null ,
                                                  load_dt timestamp not null ,
                                                  load_src varchar not null ,
                                                  hk_product_names_hashdiff uuid not null ,
                                                  primary key (h_product_pk, load_dt)
);

create table if not exists dds.s_restaurant_names(
                                                     h_restaurant_pk uuid references dds.h_restaurant(h_restaurant_pk),
                                                     name varchar not null ,
                                                     load_dt timestamp not null ,
                                                     load_src varchar not null ,
                                                     hk_restaurant_names_hashdiff uuid not null ,
                                                     primary key (h_restaurant_pk, load_dt)
);

create table if not exists dds.s_order_cost(
                                               h_order_pk uuid references dds.h_order(h_order_pk),
                                               cost decimal(19,5) not null ,
                                               payment decimal(19,5) not null ,
                                               load_dt timestamp not null ,
                                               load_src varchar not null ,
                                               hk_order_cost_hashdiff uuid not null ,
                                               primary key (h_order_pk, load_dt)
);

create table if not exists dds.s_order_status(
                                                 h_order_pk uuid references dds.h_order(h_order_pk),
                                                 status varchar not null ,
                                                 load_dt timestamp not null ,
                                                 load_src varchar not null ,
                                                 hk_order_status_hashdiff uuid not null ,
                                                 primary key (h_order_pk, load_dt)
);