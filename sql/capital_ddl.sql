CREATE TABLE "dim_date_day" (
  "id" int4 NOT NULL,
  "value" int2 NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "dim_date_month" (
  "id" int4 NOT NULL,
  "value" int2 NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "dim_date_year" (
  "id" int4 NOT NULL,
  "value" int2 NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "fact_orders" (
  "id" uuid NOT NULL DEFAULT public.uuid_generate_v4(),
  "orderid" int8 NOT NULL,
  "customerid" varchar(32) NOT NULL,
  "order_amount" float4 NOT NULL,
  "created_ts" timestamp(255) NOT NULL,
  "date_year_id" int4 NOT NULL,
  "date_month_id" int4 NOT NULL,
  "date_day_id" int4 NOT NULL,
  PRIMARY KEY ("id")
);

ALTER TABLE "dim_date_day" ADD CONSTRAINT "fk_dim_date_day_fact_orders_1" FOREIGN KEY ("id") REFERENCES "fact_orders" ("date_day_id");
ALTER TABLE "dim_date_month" ADD CONSTRAINT "fk_dim_date_month_fact_orders_1" FOREIGN KEY ("id") REFERENCES "fact_orders" ("date_month_id");
ALTER TABLE "dim_date_year" ADD CONSTRAINT "fk_dim_date_year_fact_orders_1" FOREIGN KEY ("id") REFERENCES "fact_orders" ("date_year_id");

