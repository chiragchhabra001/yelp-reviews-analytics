## Yelp Reviews Analytics (AWS S3 · Snowflake · SQL)

This project analyzes large-scale Yelp review data using a cloud-based data pipeline and SQL analytics in Snowflake.

### What Was Done
- Loaded 5GB Yelp JSON data from **AWS S3** into **Snowflake**
- Parsed semi-structured JSON using `VARIANT` data types
- Built structured review and business tables
- Applied **sentiment analysis** using a Snowflake Python UDF (TextBlob)
- Performed analytical queries using SQL

### Key Analysis
- Business count by category
- Top reviewers in the Restaurants category
- Most reviewed business categories
- Recent reviews per business
- Monthly review trends
- Percentage of 5-star reviews per business

### Tech Stack
- AWS S3  
- Snowflake  
- SQL  
- Python UDF (TextBlob)
