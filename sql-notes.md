# BigQuery

from google.cloud import bigquery
client = bigquery.Client()

## Construct a ref
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

## Fetch the dataset
dataset = client.get_dataset(dataset_ref)

tables = list(client.list_tables(dataset))
list_of_tables = [table.table_id for table in tables]
table_ref = dataset.table(<name of table>)
table = client.get_table(table_ref)
table.schema
client.list_rows(table, selected_fields=table.schema[:1], max_results=5).to_dataframe()



## Query

query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
### Seting up the query
query_job = client.query(query)
### Request
us_cities = query_job.to_dataframe()


Question: What's up with the triple quotation marks (""")?

Answer: These tell Python that everything inside them is a single string, even though we have line breaks in it. The line breaks aren't necessary, but they make it easier to read your query.

Question: Do you need to capitalize SELECT and FROM?

Answer: No, SQL doesn't care about capitalization. However, it's customary to capitalize your SQL commands, and it makes your queries a bit easier to read.


## VERY LARGE DATASET

query = """
        SELECT score, title
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = "job" 
        """
dry_run_config = bigquery.QueryJobConfig(dry_run=True)
dry_run_query_job = client.query(query, job_config=dry_run_config)
print("This query will process {} bytes.".format(dry_run_query_job.total_bytes_processed))


### Limit query data

### Only run the query if it's less than 1 MB
ONE_MB = 1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_MB)

### Set up the query (will only run if it's less than 1 MB)
safe_query_job = client.query(query, job_config=safe_config)

### API request - try to run the query, and return a pandas DataFrame
safe_query_job.to_dataframe()


## Group By

querry = """
		SELECT Animal, COUNT(ID) AS NumAnimals
		FROM 'bigquery-public-data.pet_records.pets'
		GROUP BY Animal
		HAVING COUNT(ID) > 1
		"""

## Order By

querry = """
		SELECT ID, Name, Animal
		FROM 'bigquery-public-data.pet_records.pets'
		ORDER BY ID (DESC)
		HAVING COUNT(ID) > 1
		"""

## Dates

YYYY-[M]M-[D]D
query = """
		SELECT Name, EXTRACT(DAY from  Date) AS Day
		FROM 'bigquery-public-data.pet_records.pets_with_date'
		"""

## WITH ... AS

query="""
		WITH Seniors AS
		(
			SELECT ID, Name
			FROM 'bigquery-public-data.pet_records.pets'
			WHERE Years_old > 5
		)
		SELECT ID
		FROM Seniors
		"""

## JOIN

### INNER JOIN
query = """
		SELECT p.Name AS Pet_Name, o.Name AS Owner_Name
		FROM 'bigquery-public-data.pet_records.pets' AS p
		INNER JOIN 'bigquery-public-data.pet_records.owners' AS o
			ON p.ID = o.Pet_ID
		"""

### LIKE

query = """
		SELECT *
		FROM 'bigquery-public-data.pet_records.pets'
		WHERE Name LIKE '%ipl%'  
		""" 
		#  '%ipl%' -> Ripley
		

		
		
		
		
		
