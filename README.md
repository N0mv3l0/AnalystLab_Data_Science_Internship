Act as a senior Data Scientist and technical documentation specialist.

Create a professional, engaging and technically accurate GitHub README.md for my AnalystLab Africa Track B Data Science project.

PROJECT TITLE:
Airbnb Pricing, Location and Listing Characteristics Investigation.

PROJECT OBJECTIVE:
Investigate how Airbnb advertised listing prices vary across room types within ten international cities, and explore relationships between accommodation capacity, review activity and listing ratings.

PROJECT ENVIRONMENT:
- Databricks
- Python
- Pandas
- NumPy
- Apache Spark / PySpark
- Matplotlib
- SciPy

DATASETS:
1. Listings dataset: 279,712 records and 33 columns.
2. Reviews dataset: 5,373,143 records and 4 columns.

The datasets include information about listing IDs, host information, city, neighbourhood, room type, accommodation capacity, bedrooms, advertised prices and aggregate listing ratings.

The Reviews dataset includes listing_id, review_id, date and reviewer_id.

RESEARCH QUESTIONS:
1. How does advertised listing price vary across room types within each city?
2. Is accommodation capacity associated with advertised listing price within each city?
3. Is listing-level review activity associated with the available aggregate listing rating?
4. How do listing and review activity patterns differ across cities?

STATISTICAL METHODS:
- Kruskal–Wallis H test for price differences across room types.
- Spearman rank correlation for accommodation capacity and advertised price.
- Spearman rank correlation for review count and aggregate listing rating.

DATA QUALITY:
Document the checks for duplicate listing IDs, repeated review IDs, missing values, zero prices and unmatched listing IDs.

IMPORTANT ANALYTICAL CONSIDERATIONS:
- Prices are in local currencies. Do not compare raw prices across cities.
- The Reviews dataset contains no review text, so sentiment analysis is outside the scope of this project.
- Review volume is not equivalent to occupancy or customer satisfaction.
- Statistical association does not establish causation.
- Explain the use of Spark for large-scale review aggregation and pandas for downstream analysis.

README STRUCTURE:

1. Project title and overview.
2. Project objectives.
3. Business problem and research questions.
4. Dataset description.
5. Technologies and tools.
6. Data ingestion and processing workflow.
7. Data cleaning and preparation.
8. Exploratory Data Analysis.
9. Data visualisations.
10. Statistical hypothesis testing.
11. Key findings and business insights.
12. Limitations.
13. Recommendations.
14. Repository structure.
15. How to reproduce the analysis in Databricks.
16. Future improvements.
17. Author and acknowledgements.

VISUALISATIONS:
Include Markdown image placeholders for:
- Airbnb listing counts by city.
- Price distribution by room type and city.
- Room type composition by city.
- Accommodation capacity versus advertised price.
- Historical review activity over time.
- Review count versus aggregate listing rating.

REPOSITORY STRUCTURE:
Suggest a clean structure containing:
- README.md
- notebooks/
- data/ (with instructions rather than uploading restricted or oversized datasets)
- visuals/
- reports/
- requirements.txt

WRITING STYLE:
Use clear, professional and accessible language.
Make the README suitable for recruiters, hiring managers, fellow data scientists and technical reviewers.
Explain the analytical decisions rather than simply listing Python libraries.

Do not invent findings, dataset sources, licenses, project completion status or business conclusions. Clearly distinguish preliminary findings from validated final results.

Return a complete, GitHub-ready README.md in Markdown format.
