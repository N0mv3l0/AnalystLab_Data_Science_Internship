# AnalystLab Africa -- Week 2 \| Track 2: Data Science

## Exploratory Data Analysis of E-commerce Sales and Customer Behaviour

**Business case:** Maven Fuzzy Factory\
**Environment:** Databricks\
**Technologies:** PySpark, Pandas, NumPy, Matplotlib

## Project overview

This project applies exploratory data analysis (EDA) and statistical
techniques to e-commerce data to understand website traffic, customer
purchasing behaviour, sales performance, marketing channels, product
performance and refunds.

The analysis was completed in Databricks using six existing tables.
PySpark was used to process the larger datasets, while Pandas was used
for small aggregated results and Matplotlib for visualisations. This
approach avoids collecting large source tables onto the notebook driver.

## Project objectives

-   Explore the structure, columns, data types and sample records in the
    six datasets.
-   Assess data quality through missing-value, duplicate-key and
    relationship checks.
-   Prepare date and numeric fields for analysis.
-   Examine revenue trends, product performance and customer purchasing
    behaviour.
-   Compare marketing sources and device traffic.
-   Calculate session-to-order conversion.
-   Analyse refund activity and new versus returning sessions.
-   Use descriptive statistics, correlation and visualisations to
    communicate findings.
-   Translate the analysis into practical business recommendations.

## Datasets

The notebook uses these six Databricks tables:

  --------------------------------------------------------------------------------
  Dataset                                Rows              Columns Purpose
  ---------------------- -------------------- -------------------- ---------------
  `website_sessions`                  472,871                    9 Website
                                                                   sessions,
                                                                   acquisition and
                                                                   device
                                                                   information

  `website_pageviews`               1,048,575                    4 Pageviews and
                                                                   session
                                                                   activity

  `orders_mav`                         32,313                    8 Order-level
                                                                   sales, customer
                                                                   and cost
                                                                   information

  `order_items`                        40,025                    7 Product-level
                                                                   items, prices
                                                                   and costs

  `order_item_refunds`                  1,731                    5 Refund
                                                                   transactions
                                                                   and amounts

  `products`                                4                    3 Product names
                                                                   and identifiers

  **Total**                     **1,595,519**               **36** 
  --------------------------------------------------------------------------------

## Tools and technologies

-   **Databricks:** notebook development and Spark execution.
-   **PySpark:** loading, transforming, joining and aggregating the
    datasets.
-   **Pandas:** working with small summary tables for reporting and
    plotting.
-   **NumPy:** numerical computing support.
-   **Matplotlib:** charts and visual exploration.

## Methodology

### 1. Data exploration and preparation

-   Loaded the six existing Databricks tables as Spark DataFrames.
-   Inspected row counts, column counts, column names, data types and
    sample records.
-   Checked every column for NULL values and blank strings.
-   Checked full-row duplicates and duplicate values in the expected
    primary keys.
-   Checked key relationships for unmatched records using left anti
    joins.
-   Parsed `created_at` values into timestamps and derived date/month
    fields.
-   Cast relevant sales, cost, item-count and refund measures to numeric
    types.
-   Preserved missing marketing attribution by labelling it `Unknown`.
-   Applied primary-key deduplication after checking the data.

### 2. Exploratory data analysis

The notebook examines:

-   Monthly revenue trends.
-   Product-level sales and revenue.
-   Customer order frequency and purchasing behaviour.
-   Website sessions by marketing source.
-   Website sessions by device type.
-   Session-to-order conversion rate.
-   Refund counts and refund value by product.
-   New versus returning sessions and their conversion behaviour.

### 3. Statistical analysis and visualisation

The notebook includes descriptive statistics for order, item, refund and
pageview measures. It also examines the relationship between daily
website sessions and daily orders, compares monthly sales performance,
compares product revenue with refund value, identifies popular website
pages and plots daily revenue trends.

Small aggregated Spark results are converted to Pandas for Matplotlib
charts; the large source datasets remain in Spark.

### 4. Interpretation and recommendations

The final analysis brings together business metrics and trends to
support recommendations about marketing performance, device experience,
product performance, customer retention, conversion opportunities and
refund investigation.

## Key findings from the notebook

The following values are taken from the recorded notebook outputs:

  Measure                                                  Finding
  ------------------------------------------- --------------------
  Unique orders                                             32,313
  Total order-level revenue                     **\$1,938,509.75**
  Average order value                                  **\$59.99**
  Unique website sessions                                  472,871
  Sessions with an order                                    32,313
  Session-to-order conversion rate                       **6.83%**
  Refund transactions                                        1,731
  Total refund value                               **\$85,338.69**
  Daily sessions/orders Pearson correlation              **0.956**

Additional findings:

-   Highest revenue month in the observed data: **December 2014 --
    \$144,823.02**.
-   Lowest revenue month in the observed data: **March 2012 --
    \$2,999.40**.
-   Highest product-level revenue: **The Original Mr. Fuzzy --
    \$1,211,057.74**.
-   Marketing source with the most recorded sessions: **gsearch --
    316,035 sessions**.
-   Device with the most recorded sessions: **desktop -- 327,027
    sessions**.

**Interpretation note:** The correlation of 0.956 indicates a strong
positive association between daily sessions and daily orders.
Correlation does not establish causation. Order-level revenue from
`orders_mav` and product-level revenue from `order_items` are different
measures and should not be added together. Refund value is not, by
itself, evidence of a product defect.

## Data quality results

-   No NULL values were detected in the 36 inspected columns.
-   No blank strings were detected in the inspected text columns.
-   No full-row duplicates or duplicate primary-key groups were
    detected.
-   The relationship checks reported zero unmatched records for
    pageviews-to-sessions, orders-to-sessions, items-to-orders,
    items-to-products and refunds-to-order-items.
-   The date parsing checks reported zero NULL `created_at` values for
    all six datasets.

## Repository contents

Suggested repository structure:

``` text
analystlab-week2-ecommerce-eda/
├── README.md
├── AnalystLab_Week2_Track2_Databricks_PySpark_Pandas_Matplotlib.ipynb
├── AnalystLab_Week2_Track2_Databricks_PySpark_Pandas_Matplotlib.py
└── visualisations/
    ├── monthly_revenue.png
    ├── product_revenue.png
    ├── customer_order_frequency.png
    └── ...
```

Add the notebook, Python export and visualisation images to the
repository if they are part of your final submission. The
`visualisations/` folder above is a suggested structure; include only
files you have actually exported.

## How to run

1.  Open the notebook in a Databricks workspace.
2.  Ensure the six source tables are available in the catalog and schema
    referenced by the notebook.
3.  If your catalog or schema names differ, update the table references
    in the data-loading cell.
4.  Run the notebook cells in order.
5.  Review the outputs and charts, then complete the evidence-based
    insight and recommendation section using the results produced in
    your own workspace.

The notebook references the tables
`workspace.e-commerce.website_sessions`,
`workspace.e-commerce.website_pageviews`,
`workspace.e-commerce.orders_mav`, `workspace.e-commerce.order_items`,
`workspace.e-commerce.order_item_refunds` and
`workspace.e-commerce.products`. The schema contains a hyphen, so the
notebook uses backticks around the identifiers.

## Limitations and interpretation

-   Missing marketing attribution is labelled `Unknown`; it is not
    assigned to a known channel.
-   Order-level revenue and product-level item revenue are separate
    measures.
-   Refund value alone does not establish why a product was refunded;
    refund reasons are not included in the supplied fields.
-   Correlation between traffic and orders does not prove causation.
-   The supplied products table contains product IDs, names and creation
    dates, but no product categories.
-   Historical patterns do not guarantee future performance.

## Learning outcomes

Through this project, I practised working with multi-table e-commerce
data, using PySpark for scalable data processing, validating data
quality, preparing timestamps and measures, performing EDA, calculating
business metrics, visualising aggregated results with Matplotlib and
communicating data-informed findings.

## Acknowledgement

Completed as part of **AnalystLab Africa -- Week 2, Track 2: Data
Science**.
