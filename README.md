# pydata-examples

Notebook examples with the PyData libraries such as pandas, Datafusion, DuckDB, and Polars.

## Setup

There are environment files that you can use to create conda environments with all the dependencies for this project.

## Queries

DataFusion:

```
def q1(ctx):
    return ctx.sql("select id1, sum(v1) as v1 from x group by id1").collect()


def q2(ctx):
    return ctx.sql("select id1, id2, sum(v1) as v1 from x group by id1, id2").collect()


def q3(ctx):
    return ctx.sql("select count(distinct(id3)) from x").collect()
    # return ctx.sql("select id3, sum(v1) as v1, avg(v3) as v3 from x group by id3").collect()


def q4(ctx):
    return ctx.sql("select id4, avg(v1) as v1, avg(v2) as v2, avg(v3) as v3 from x group by id4").collect()


def q5(ctx):
    return ctx.sql("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").collect()


def q6(ctx):
    return ctx.sql("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5").collect()


def q7(ctx):
    return ctx.sql("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3").collect()


def q8(ctx):
    return ctx.sql("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2").collect()


def q9(ctx):
    return ctx.sql("select id2, id4, power(corr(v1, v2), 2) as r2 from x group by id2, id4").collect()


def q10(ctx):
    return ctx.sql("select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6").collect()
```

Polars

```
def q1(df):
    return df.groupby("id1").agg(pl.sum("v1"))


def q2(df):
    return df.groupby(["id1", "id2"]).agg(pl.sum("v1"))


def q3(df):
    return df.groupby("id3").agg([pl.sum("v1"), pl.mean("v3")])


def q4(df):
    return (
        df.groupby("id4").agg([pl.mean("v1"), pl.mean("v2"), pl.mean("v3")])
    )


def q5(df):
    return df.groupby("id6").agg([pl.sum("v1"), pl.sum("v2"), pl.sum("v3")])


def q6(df):
    return (
        df.groupby(["id4", "id5"])
        .agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")])

    )


def q7(df):
    return (
        df.groupby("id3")
        .agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")])
    )


def q8(df):
    return (
        df.drop_nulls("v3")
        .sort("v3", reverse=True)
        .groupby("id6")
        .agg(pl.col("v3").head(2).alias("largest2_v3"))
        .explode("largest2_v3")
    )


def q9(df):
    return (
        df.groupby(["id2", "id4"])
        .agg((pl.pearson_corr("v1", "v2") ** 2).alias("r2"))
    )


def q10(df):
    return (
        df.groupby(["id1", "id2", "id3", "id4", "id5", "id6"])
        .agg([pl.sum("v3").alias("v3"), pl.count("v1").alias("count")])
    )
```




