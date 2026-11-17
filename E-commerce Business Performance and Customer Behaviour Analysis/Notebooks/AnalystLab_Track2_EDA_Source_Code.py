"""
AnalystLab Africa | Week 2 | Track 2: Data Science
Exploratory Data Analysis of E-commerce Sales and Customer Behaviour
Databricks PySpark source script. Run in a Databricks Python notebook/job
with access to workspace.e-commerce source tables.
"""
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Load six source tables. Keep large tables in Spark.
sessions = spark.read.table("`workspace`.`e-commerce`.`website_sessions`")
pageviews = spark.read.table("`workspace`.`e-commerce`.`website_pageviews`")
orders = spark.read.table("`workspace`.`e-commerce`.`orders_mav`")
order_items = spark.read.table("`workspace`.`e-commerce`.`order_items`")
refunds = spark.read.table("`workspace`.`e-commerce`.`order_item_refunds`")
products = spark.read.table("`workspace`.`e-commerce`.`products`")
datasets = {"website_sessions":sessions,"website_pageviews":pageviews,
            "orders_mav":orders,"order_items":order_items,
            "order_item_refunds":refunds,"products":products}

# Explore dimensions, columns and data types.
for name,df in datasets.items():
    print(f"\\n===== {name} =====")
    print("Rows:",df.count(),"Columns:",len(df.columns))
    print("Column names:",df.columns)
    print("Data types:",df.dtypes)
    display(df.limit(5))

# Missing and blank values.
missing_results=[]
for name,df in datasets.items():
    exprs=[]
    for col_name in df.columns:
        exprs.append(F.sum(F.when(F.col(col_name).isNull(),1).otherwise(0)).alias(col_name))
        if dict(df.dtypes)[col_name]=="string":
            exprs.append(F.sum(F.when(F.trim(F.col(col_name))=="",1).otherwise(0)).alias(col_name+"_blank"))
    result=df.agg(*exprs).first().asDict()
    for col_name in df.columns:
        missing_results.append({"dataset":name,"column":col_name,
            "null_values":result.get(col_name) or 0,
            "blank_values":result.get(col_name+"_blank",0) or 0})
missing_df=pd.DataFrame(missing_results)
display(missing_df)

# Primary-key duplicate checks.
primary_keys={"website_sessions":"website_session_id",
 "website_pageviews":"website_pageview_id","orders_mav":"order_id",
 "order_items":"order_item_id","order_item_refunds":"order_item_refund_id",
 "products":"product_id"}
duplicate_results=[]
for name,df in datasets.items():
    key=primary_keys[name]
    groups=df.groupBy(key).count().filter(F.col("count")>1)
    excess=groups.select(F.sum(F.col("count")-1).alias("n")).first()["n"] or 0
    duplicate_results.append({"dataset":name,"rows":df.count(),
        "full_row_duplicates":df.count()-df.dropDuplicates().count(),
        "duplicate_key_groups":groups.count(),"excess_key_records":excess})
display(pd.DataFrame(duplicate_results))

# Referential integrity checks.
session_ids=sessions.select("website_session_id").distinct()
order_ids=orders.select("order_id").distinct()
item_ids=order_items.select("order_item_id").distinct()
product_ids=products.select("product_id").distinct()
orphan_checks={
 "pageviews without a session":pageviews.join(session_ids,"website_session_id","left_anti").count(),
 "orders without a session":orders.join(session_ids,"website_session_id","left_anti").count(),
 "items without an order":order_items.join(order_ids,"order_id","left_anti").count(),
 "items without a product":order_items.join(product_ids,"product_id","left_anti").count(),
 "refunds without an order item":refunds.join(item_ids,"order_item_id","left_anti").count()}
for label,n in orphan_checks.items(): print(f"{label}: {n:,}")

# Parse timestamps and cast numeric measures.
def parse_created_at(df):
    return df.withColumn("created_at",F.coalesce(
        F.to_timestamp("created_at","M/d/yyyy H:mm"),
        F.to_timestamp("created_at","M/d/yyyy H:mm:ss"),
        F.to_timestamp("created_at")))
sessions,pageviews,orders,order_items,refunds,products=map(
    parse_created_at,[sessions,pageviews,orders,order_items,refunds,products])
orders=(orders.withColumn("price_usd",F.col("price_usd").cast("double"))
 .withColumn("cogs_usd",F.col("cogs_usd").cast("double"))
 .withColumn("items_purchased",F.col("items_purchased").cast("int")))
order_items=(order_items.withColumn("price_usd",F.col("price_usd").cast("double"))
 .withColumn("cogs_usd",F.col("cogs_usd").cast("double")))
refunds=refunds.withColumn("refund_amount_usd",F.col("refund_amount_usd").cast("double"))
sessions=sessions.withColumn("session_date",F.to_date("created_at")).withColumn("session_month",F.date_format("created_at","yyyy-MM"))
pageviews=pageviews.withColumn("pageview_date",F.to_date("created_at"))
orders=orders.withColumn("order_date",F.to_date("created_at")).withColumn("order_month",F.date_format("created_at","yyyy-MM"))
order_items=order_items.withColumn("item_date",F.to_date("created_at"))
refunds=refunds.withColumn("refund_date",F.to_date("created_at"))
for label,df in [("sessions",sessions),("pageviews",pageviews),("orders",orders),
                 ("order_items",order_items),("refunds",refunds),("products",products)]:
    print(label,"unparsed dates:",df.filter(F.col("created_at").isNull()).count())

# Deduplicate expected primary keys (no duplicate keys were found in the saved run).
sessions=sessions.dropDuplicates(["website_session_id"])
pageviews=pageviews.dropDuplicates(["website_pageview_id"])
orders=orders.dropDuplicates(["order_id"])
order_items=order_items.dropDuplicates(["order_item_id"])
refunds=refunds.dropDuplicates(["order_item_refund_id"])
products=products.dropDuplicates(["product_id"])
for col_name in ["utm_source","utm_campaign","utm_content","http_referer"]:
    sessions=sessions.withColumn(col_name,F.when(
        F.col(col_name).isNull()|(F.trim(F.col(col_name))==""),"Unknown").otherwise(F.col(col_name)))

# Monthly revenue: order-level revenue avoids multiplication from item joins.
monthly_revenue=(orders.filter(F.col("order_month").isNotNull()).groupBy("order_month")
 .agg(F.sum("price_usd").alias("revenue_usd"),
      F.countDistinct("order_id").alias("total_orders")).orderBy("order_month"))
display(monthly_revenue)
monthly_pd=monthly_revenue.toPandas()
plt.figure(figsize=(12,5)); plt.plot(monthly_pd["order_month"],monthly_pd["revenue_usd"],marker="o")
plt.title("Monthly Revenue"); plt.xlabel("Month"); plt.ylabel("Revenue (USD)")
plt.xticks(rotation=45); plt.grid(alpha=.3); plt.tight_layout(); plt.show()

# Product sales and revenue.
product_performance=(order_items.groupBy("product_id")
 .agg(F.countDistinct("order_item_id").alias("units_sold"),
      F.sum("price_usd").alias("product_revenue_usd"),
      F.countDistinct("order_id").alias("distinct_orders"))
 .join(products.select("product_id","product_name"),"product_id","left")
 .select("product_id","product_name","units_sold","product_revenue_usd","distinct_orders")
 .orderBy(F.desc("product_revenue_usd")))
display(product_performance)
product_pd=product_performance.toPandas()
plt.figure(figsize=(10,5)); plt.barh(product_pd.head(5)["product_name"],product_pd.head(5)["product_revenue_usd"])
plt.title("Top Products by Revenue"); plt.xlabel("Product Revenue (USD)"); plt.tight_layout(); plt.show()

# Customer purchasing patterns.
orders_per_customer=orders.groupBy("user_id").agg(
 F.countDistinct("order_id").alias("number_of_orders"),
 F.sum("price_usd").alias("customer_revenue_usd"))
display(orders_per_customer.orderBy(F.desc("number_of_orders")).limit(20))
display(orders.select("price_usd","cogs_usd","items_purchased").describe())
frequency=orders_per_customer.groupBy("number_of_orders").agg(F.count("*").alias("customers")).orderBy("number_of_orders")
frequency_pd=frequency.toPandas()
plt.figure(figsize=(9,5)); plt.bar(frequency_pd["number_of_orders"],frequency_pd["customers"])
plt.title("Customer Order Frequency"); plt.xlabel("Orders per Customer"); plt.ylabel("Customers"); plt.tight_layout(); plt.show()

# Marketing source and device traffic.
sessions_by_source=sessions.groupBy("utm_source").agg(F.countDistinct("website_session_id").alias("session_count")).orderBy(F.desc("session_count"))
display(sessions_by_source)
source_pd=sessions_by_source.toPandas()
plt.figure(figsize=(10,5)); plt.bar(source_pd["utm_source"],source_pd["session_count"])
plt.title("Sessions by Marketing Source"); plt.xlabel("Source"); plt.ylabel("Sessions"); plt.xticks(rotation=35); plt.tight_layout(); plt.show()
sessions_by_device=sessions.groupBy("device_type").agg(F.countDistinct("website_session_id").alias("session_count")).orderBy(F.desc("session_count"))
display(sessions_by_device)
device_pd=sessions_by_device.toPandas()
plt.figure(figsize=(7,5)); plt.bar(device_pd["device_type"],device_pd["session_count"])
plt.title("Sessions by Device"); plt.xlabel("Device"); plt.ylabel("Sessions"); plt.tight_layout(); plt.show()

# Overall session-to-order conversion.
total_sessions=sessions.select("website_session_id").distinct().count()
converting_sessions=orders.select("website_session_id").distinct().count()
conversion_rate_pct=converting_sessions/total_sessions*100 if total_sessions else 0
print(f"Sessions: {total_sessions:,}; converting: {converting_sessions:,}; conversion: {conversion_rate_pct:.2f}%")

# Refunds by product.
refund_details=(refunds.join(order_items.select("order_item_id","product_id"),"order_item_id","left")
 .join(products.select("product_id","product_name"),"product_id","left"))
refund_by_product=refund_details.groupBy("product_id","product_name").agg(
 F.countDistinct("order_item_refund_id").alias("refund_count"),
 F.sum("refund_amount_usd").alias("refund_value_usd")).orderBy(F.desc("refund_value_usd"))
display(refund_by_product)
refund_pd=refund_by_product.toPandas()
plt.figure(figsize=(10,5)); plt.bar(refund_pd["product_name"],refund_pd["refund_value_usd"])
plt.title("Refund Value by Product"); plt.xlabel("Product"); plt.ylabel("Refund Value (USD)")
plt.xticks(rotation=30,ha="right"); plt.tight_layout(); plt.show()

# New versus returning sessions and conversion.
sessions_typed=sessions.withColumn("session_type",F.when(F.col("is_repeat_session").cast("int")==1,"Returning").otherwise("New"))
converted_ids=orders.select("website_session_id").distinct().withColumn("converted",F.lit(1))
session_outcomes=sessions_typed.join(converted_ids,"website_session_id","left").fillna({"converted":0})
conversion_by_type=session_outcomes.groupBy("session_type").agg(
 F.countDistinct("website_session_id").alias("sessions"),
 F.countDistinct(F.when(F.col("converted")==1,F.col("website_session_id"))).alias("converted_sessions")
).withColumn("conversion_rate_pct",F.col("converted_sessions")/F.col("sessions")*100)
display(conversion_by_type)

# Daily traffic and order relationship.
daily_sessions=sessions.filter(F.col("session_date").isNotNull()).groupBy("session_date").agg(F.countDistinct("website_session_id").alias("sessions"))
daily_orders=orders.filter(F.col("order_date").isNotNull()).groupBy(F.col("order_date").alias("session_date")).agg(F.countDistinct("order_id").alias("orders"))
daily_traffic_orders=daily_sessions.join(daily_orders,"session_date","left").fillna({"orders":0}).orderBy("session_date")
display(daily_traffic_orders)
print("Daily sessions/orders Pearson correlation:",daily_traffic_orders.stat.corr("sessions","orders"))

# Additional descriptive statistics, monthly comparison and top pages.
display(order_items.select("price_usd","cogs_usd").describe())
display(refunds.select("refund_amount_usd").describe())
pageviews_per_session=pageviews.groupBy("website_session_id").agg(F.count("website_pageview_id").alias("pageview_count"))
display(pageviews_per_session.select("pageview_count").describe())
monthly_performance=orders.groupBy("order_month").agg(
 F.countDistinct("order_id").alias("total_orders"),
 F.sum("price_usd").alias("total_revenue_usd"),
 F.avg("price_usd").alias("average_order_value_usd")).orderBy("order_month")
display(monthly_performance)
top_pages=pageviews.groupBy("pageview_url").agg(F.count("website_pageview_id").alias("pageviews")).orderBy(F.desc("pageviews")).limit(10)
display(top_pages)
