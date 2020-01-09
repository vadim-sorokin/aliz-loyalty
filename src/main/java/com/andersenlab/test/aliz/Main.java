package com.andersenlab.test.aliz;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class Main {
	private static final String FULL_VISITOR_ID_FIELD = "fullVisitorId";
	private static final String PRODUCTS_FIELD = "products";

	public static void main(String[] args) throws InterruptedException {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("WITH consecutiveWeeks as (\r\n"
				+ "select fullVisitorId, productSKU, max(date) as max_week,count(*) as consecutiveWeeksCount   from (\r\n"
				+ "SELECT\r\n" + "  A.fullVisitorId, date, \r\n"
				+ "  p.productSKU as productSKU  , p.productQuantity,\r\n" + "\r\n"
				+ " DATE_DIFF(PARSE_DATE(\"%Y%m%d\", date), LAG(PARSE_DATE(\"%Y%m%d\", date)) OVER(PARTITION BY A.fullVisitorId, p.productSKU ORDER BY date), WEEK) as weeks\r\n"
				+ "FROM\r\n"
				+ "  `data-to-insights.ecommerce.web_analytics` as A,  UNNEST(hits) as h, UNNEST(h.product) as p\r\n"
				+ "join (SELECT\r\n" + "  fullVisitorId, p.productSKU as productSKU , count(*) as cnt\r\n" + "FROM\r\n"
				+ "    `data-to-insights.ecommerce.web_analytics`, UNNEST(hits) as h, UNNEST(h.product) as p\r\n"
				+ "WHERE\r\n" + "  p.productQuantity IS NOT NULL and p.productRevenue  is not NULL \r\n"
				+ "group by fullVisitorId, productSKU) as B\r\n" + "on A.fullVisitorId=B.fullVisitorId and \r\n"
				+ "p.productSKU=B.productSKU\r\n" + "WHERE\r\n"
				+ "  B.cnt >1  and p.productQuantity IS NOT NULL and p.productRevenue  is not NULL \r\n"
				+ "order by  A.fullVisitorId, date, \r\n" + "  p.productSKU) C\r\n" + "where weeks=1\r\n"
				+ "group by  fullVisitorId, productSKU\r\n" + "LIMIT 10)\r\n" + "\r\n" + "\r\n"
				+ "select fullVisitorId,\r\n" + "ARRAY_AGG(DISTINCT cat_id IGNORE NULLS) products \r\n" + "from (\r\n"
				+ "select A.fullVisitorId as fullVisitorId , p.productSKU as productSKU ,  p.v2ProductName as v2ProductName, \r\n"
				+ "CAST(sum(p.productQuantity) AS STRING) as quantity , CAST(sum(p.productRevenue) AS STRING) as totalValue, \r\n"
				+ "CAST(max(B.max_week) AS STRING) as lastWeek , CAST(sum(B.consecutiveWeeksCount) AS STRING) as consecutiveWeeksCount  \r\n"
				+ "from `data-to-insights.ecommerce.web_analytics` as A, UNNEST(hits) as h, UNNEST(h.product) as p\r\n"
				+ "join consecutiveWeeks B\r\n" + "on A.fullVisitorId=B.fullVisitorId\r\n"
				+ "and p.productSKU=B.productSKU\r\n"
				+ "where p.productQuantity IS NOT NULL and p.productRevenue  is not NULL \r\n"
				+ "group by A.fullVisitorId, p.productSKU, p.v2ProductName ) C,\r\n"
				+ "UNNEST([productSKU,v2ProductName,quantity,totalValue,lastWeek,consecutiveWeeksCount]) cat_id\r\n"
				+ "group by fullVisitorId").build();

		// Create a job ID so that we can safely retry.
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		// Wait for the query to complete.
		queryJob = queryJob.waitFor();

		// Check for errors
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
			// You can also look at queryJob.getStatus().getExecutionErrors() for all
			// errors, not just the latest one.
			throw new RuntimeException(queryJob.getStatus().getError().toString());
		}

		// Get the results.
		TableResult result = queryJob.getQueryResults();

		processResult(result);
	}

	private static void processResult(final TableResult pTableResult) {
		final List<String> purchases = new ArrayList<>();

		for (final FieldValueList row : pTableResult.iterateAll()) {
			final StringBuilder purchase = new StringBuilder();

			// Append visitor id.
			final FieldValue fullVisitorId = row.get(FULL_VISITOR_ID_FIELD);
			purchase.append(fullVisitorId.getStringValue());

			// Append delimiter.
			purchase.append(" - ");

			// Append product name.
			final FieldValue products = row.get(PRODUCTS_FIELD);
			final FieldValue productName = products.getRepeatedValue().get(1);
			purchase.append(productName.getStringValue());

			purchases.add(purchase.toString());
		}

		printResult(purchases);
	}

	private static void printResult(final List<String> pResult) {
		pResult.stream().forEach(System.out::println);
	}
}
