# aliz-loyalty
Aliz-loyalty challenge implementation.

BigQuery script description. Two queries:
1) "consecutiveWeeks" - to get a list if visitors and SKUs purchased at least in 1 conectutive week:
    a) nested query ("B" table) - select visitors + SKU purchased more than 1 item
    b) nested query ("A" table) - using LAG function calculatio number of week between consecutive  transactions
    c) join both queries and filter records where weeks =1 (2 consecutive  weeks)
    For testing there is "LIMIT 10" statement that should be removed to get full result
2) join  "web_analytics" table with "consecutiveWeeks" table by fullVisitorId and productSKU 
     to get total values and convert it to "products" ARRAY (ARRAY_AGG)

#standardSQL
WITH consecutiveWeeks as (
select fullVisitorId, productSKU, max(date) as max_week,count(*) as consecutiveWeeksCount   from (
SELECT
  A.fullVisitorId, date, 
  p.productSKU as productSKU  , p.productQuantity,

 DATE_DIFF(PARSE_DATE("%Y%m%d", date), LAG(PARSE_DATE("%Y%m%d", date)) OVER(PARTITION BY A.fullVisitorId, p.productSKU ORDER BY date), WEEK) as weeks
FROM
  `data-to-insights.ecommerce.web_analytics` as A,  UNNEST(hits) as h, UNNEST(h.product) as p
join (SELECT
  fullVisitorId, p.productSKU as productSKU , count(*) as cnt
FROM
    `data-to-insights.ecommerce.web_analytics`, UNNEST(hits) as h, UNNEST(h.product) as p
WHERE
  p.productQuantity IS NOT NULL and p.productRevenue  is not NULL 
group by fullVisitorId, productSKU) as B
on A.fullVisitorId=B.fullVisitorId and 
p.productSKU=B.productSKU
WHERE
  B.cnt >1  and p.productQuantity IS NOT NULL and p.productRevenue  is not NULL 
order by  A.fullVisitorId, date, 
  p.productSKU) C
where weeks=1
group by  fullVisitorId, productSKU
LIMIT 10)

select fullVisitorId,
ARRAY_AGG(DISTINCT cat_id IGNORE NULLS) products 
from (
select A.fullVisitorId as fullVisitorId , p.productSKU as productSKU ,  p.v2ProductName as v2ProductName, 
CAST(sum(p.productQuantity) AS STRING) as quantity , CAST(sum(p.productRevenue) AS STRING) as totalValue, 
CAST(max(B.max_week) AS STRING) as lastWeek , CAST(sum(B.consecutiveWeeksCount) AS STRING) as consecutiveWeeksCount  
from `data-to-insights.ecommerce.web_analytics` as A, UNNEST(hits) as h, UNNEST(h.product) as p
join consecutiveWeeks B
on A.fullVisitorId=B.fullVisitorId
and p.productSKU=B.productSKU
where p.productQuantity IS NOT NULL and p.productRevenue  is not NULL 
group by A.fullVisitorId, p.productSKU, p.v2ProductName ) C,
UNNEST([productSKU,v2ProductName,quantity,totalValue,lastWeek,consecutiveWeeksCount]) cat_id
group by fullVisitorId

================================================================================

How to run the application.

System prerequisites:
1. Java 11
2. Maven 3.3+
3. Create an environment variable into a system GOOGLE_APPLICATION_CREDENTIALS which is aimed to the file gcp.json which is provided within the project archive ({root}/secret/gcp.json).

How to run:
1. Build by Maven
2. Run as simple Java application


