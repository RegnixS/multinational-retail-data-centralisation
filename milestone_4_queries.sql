
/* Task 1 Which countries we currently operate in and which country now has the most stores */
SELECT country_code, 
		COUNT(country_code) AS total_no_stores
	FROM dim_store_details
	WHERE locality <> 'N/A'
	GROUP BY country_code
	ORDER BY COUNT(country_code) DESC
	;

/* Task 2 Which locations have the most stores currently */
SELECT locality, 
		COUNT(locality) AS total_no_stores
	FROM dim_store_details
	GROUP BY locality
	HAVING COUNT(locality) >= 10
	ORDER BY COUNT(locality) DESC
	;

/* Task 3 Which months have produced the most sales */
SELECT ROUND(SUM(product_quantity * product_price):: NUMERIC, 2) AS total_sales,
		month
	FROM orders_table o
	JOIN dim_date_times dt ON o.date_uuid = dt.date_uuid
	JOIN dim_products p ON o.product_code = p.product_code
	GROUP BY month
	ORDER BY total_sales DESC
	LIMIT 6
	;

/* Task 4 How many products were sold and the amount of sales made for online and offline purchases */
SELECT count(product_code) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count,
		(CASE WHEN store_type = 'Web Portal' THEN 'Web' ELSE 'Offline' END) AS location 
	FROM orders_table o
	JOIN dim_store_details s ON o.store_code = s.store_code
	GROUP BY location
	ORDER BY location DESC
	;

/* Task 5 which of the different store types has generated the most revenue */
SELECT store_type, 
		ROUND(SUM(product_quantity * product_price):: NUMERIC, 2) AS total_sales,
		ROUND(SUM(product_quantity * product_price):: NUMERIC / SUM(SUM(product_quantity * product_price):: NUMERIC) OVER () * 100 , 2) AS "percentage_total(%)"
	FROM orders_table o
	JOIN dim_store_details s ON o.store_code = s.store_code
	JOIN dim_products p ON o.product_code = p.product_code
	GROUP BY store_type
	ORDER BY total_sales DESC
	;

/* Task 6 Which months in which years have had the most sales historically */
SELECT ROUND(SUM(product_quantity * product_price):: NUMERIC, 2) AS total_sales,
		year,
		month
	FROM orders_table o
	JOIN dim_date_times dt ON o.date_uuid = dt.date_uuid
	JOIN dim_products p ON o.product_code = p.product_code
	GROUP BY year, month
	ORDER BY total_sales DESC
	LIMIT 10
	;

/* Task 7 Staff numbers in each of the countries the company sells in */
SELECT SUM(staff_numbers) AS total_staff_numbers,
		country_code
	FROM dim_store_details
	GROUP BY country_code
	ORDER BY total_staff_numbers DESC
	;

/* Task 8 Which type of store is generating the most sales in Germany */
SELECT ROUND(SUM(product_quantity * product_price):: NUMERIC, 2) AS total_sales,
		store_type,
		MAX(country_code) AS country_code
	FROM orders_table o
	JOIN dim_store_details s ON o.store_code = s.store_code
	JOIN dim_products p ON o.product_code = p.product_code
	WHERE country_code = 'DE'
	GROUP BY store_type
	ORDER BY total_sales
	;


/* Task 9 Average time taken between each sale grouped by year */
WITH cte1 AS (
	SELECT year,
			(year || '-' || month || '-' || day || ' ' || timestamp):: TIMESTAMP AS date_time
		FROM dim_date_times
		ORDER BY date_time DESC
	), cte2 AS ( 
	SELECT year,
			date_time - LEAD(date_time) OVER() AS diff
		FROM cte1
	)
SELECT year,
		('"hours": ' || SPLIT_PART(AVG(diff):: VARCHAR, ':', 1):: NUMERIC ||
		', "minutes": ' || SPLIT_PART(AVG(diff):: VARCHAR, ':', 2):: NUMERIC ||
		' , "seconds": ' || SUBSTRING(AVG(diff):: VARCHAR, 7, 2):: NUMERIC ||
		' , "milliseconds": ' || SPLIT_PART(AVG(diff):: VARCHAR, '.', 2):: NUMERIC) AS actual_time_taken
	FROM cte2
	GROUP BY year
	ORDER BY AVG(diff) DESC
	LIMIT 5
	;