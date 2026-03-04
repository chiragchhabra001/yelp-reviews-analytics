/*
Project: Yelp Reviews Analytics
Author: Chirag Chhabra
Tech Stack: AWS S3, Snowflake, SQL, Python

Description:
- Ingests Yelp JSON data from AWS S3 into Snowflake
- Transforms semi-structured data using VARIANT
- Performs sentiment analysis on reviews
- Answers key business questions using SQL
*/

-- =====================================================
-- 1. RAW DATA INGESTION (JSON → VARIANT)
-- =====================================================

CREATE OR REPLACE TABLE yelp_reviews (
    review_text VARIANT
);

COPY INTO yelp_reviews
FROM 's3://chirag-c-s3-bucket/yelp/'
FILE_FORMAT = (TYPE = JSON);
-- AWS credentials
-- Data loaded using secure IAM-based access

CREATE OR REPLACE TABLE yelp_businesses (
    business_text VARIANT
);

COPY INTO yelp_businesses
FROM 's3://chirag-c-s3-bucket/yelp/yelp_academic_dataset_business.json'
FILE_FORMAT = (TYPE = JSON);
-- AWS credentials

-- =====================================================
-- 2. SENTIMENT ANALYSIS FUNCTION
-- =====================================================

-- Python UDF to classify review sentiment
CREATE OR REPLACE FUNCTION textblob_sentiment(review STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('textblob')
HANDLER = 'analyze'
AS
$$
from textblob import TextBlob

def analyze(review):
    if review is None:
        return 'Neutral'

    polarity = TextBlob(review).sentiment.polarity

    if polarity > 0:
        return 'Positive'
    elif polarity < 0:
        return 'Negative'
    else:
        return 'Neutral'
$$;

-- =====================================================
-- 3. DATA TRANSFORMATION
-- =====================================================

-- Structured reviews table
CREATE OR REPLACE TABLE tbl_yelp_reviews AS
SELECT
    review_text:business_id::STRING AS business_id,
    review_text:review_id::STRING   AS review_id,
    review_text:date::DATE          AS review_date,
    review_text:user_id::STRING     AS user_id,
    review_text:stars::NUMBER       AS review_stars,
    review_text:text::STRING        AS review_text,
    textblob_sentiment(review_text:text::STRING) AS sentiment_analysis
FROM yelp_reviews;

-- Structured businesses table
CREATE OR REPLACE TABLE tbl_yelp_businesses AS
SELECT
    business_text:business_id::STRING AS business_id,
    business_text:name::STRING        AS name,
    business_text:categories::STRING  AS categories,
    business_text:city::STRING        AS city,
    business_text:state::STRING       AS state,
    business_text:stars::DECIMAL(2,1) AS stars
FROM yelp_businesses;

-- =====================================================
-- 5. ANALYSIS QUERIES
-- =====================================================

-- Q1: Number of businesses in each category
WITH category_expanded AS (
    SELECT
        business_id,
        TRIM(cat.value::STRING) AS category
    FROM tbl_yelp_businesses,
         LATERAL SPLIT_TO_TABLE(categories, ',') cat
)
SELECT
    category,
    COUNT(DISTINCT business_id) AS no_of_businesses
FROM category_expanded
GROUP BY category
ORDER BY no_of_businesses DESC;

-- Q2: Top 10 users reviewing the most Restaurant businesses
WITH restaurant_businesses AS (
    SELECT DISTINCT business_id
    FROM tbl_yelp_businesses,
         LATERAL SPLIT_TO_TABLE(categories, ',') cat
    WHERE TRIM(cat.value::STRING) = 'Restaurants'
),
restaurant_reviews AS (
    SELECT
        r.user_id,
        r.business_id
    FROM tbl_yelp_reviews r
    JOIN restaurant_businesses rb
        ON r.business_id = rb.business_id
)
SELECT
    user_id,
    COUNT(DISTINCT business_id) AS businesses_reviewed
FROM restaurant_reviews
GROUP BY user_id
ORDER BY businesses_reviewed DESC
LIMIT 10;

-- Q3: Most reviewed business categories
SELECT
    TRIM(cat.value::STRING) AS category,
    COUNT(r.review_id) AS total_reviews
FROM tbl_yelp_reviews r
JOIN tbl_yelp_businesses b
    ON r.business_id = b.business_id,
     LATERAL SPLIT_TO_TABLE(b.categories, ',') cat
GROUP BY category
ORDER BY total_reviews DESC;

-- Q4: Top 3 most recent reviews per business
WITH ranked_reviews AS (
    SELECT
        r.business_id,
        b.name AS business_name,
        r.review_id,
        r.review_date,
        r.review_stars,
        r.sentiment_analysis,
        ROW_NUMBER() OVER (
            PARTITION BY r.business_id
            ORDER BY r.review_date DESC
        ) AS rn
    FROM tbl_yelp_reviews r
    JOIN tbl_yelp_businesses b
        ON r.business_id = b.business_id
)
SELECT *
FROM ranked_reviews
WHERE rn <= 3
ORDER BY business_id, review_date DESC;

-- Q5: Month with the highest number of reviews
SELECT
    TO_CHAR(review_date, 'YYYY-MM') AS review_month,
    COUNT(*) AS total_reviews
FROM tbl_yelp_reviews
GROUP BY review_month
ORDER BY total_reviews DESC;

-- Q6: Percentage of 5-star reviews per business
SELECT
    b.business_id,
    b.name AS business_name,
    COUNT(*) AS total_reviews,
    SUM(CASE WHEN r.review_stars = 5 THEN 1 ELSE 0 END) AS five_star_reviews,
    ROUND(
        (SUM(CASE WHEN r.review_stars = 5 THEN 1 ELSE 0 END) * 100.0)
        / COUNT(*), 2
    ) AS five_star_percentage
FROM tbl_yelp_reviews r
JOIN tbl_yelp_businesses b
    ON r.business_id = b.business_id
GROUP BY b.business_id, b.name
ORDER BY five_star_percentage DESC;
