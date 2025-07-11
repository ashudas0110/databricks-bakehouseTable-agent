--- Use the python , notebook code to create dataframe and tables within my system 
b_df_reviews     = spark.table("samples.bakehouse.media_gold_reviews_chunked")
b_df_customers   = spark.table("samples.bakehouse.sales_customers")
b_df_franchises  = spark.table("samples.bakehouse.sales_franchises")
b_df_suppliers   = spark.table("samples.bakehouse.sales_suppliers")
b_df_transactions = spark.table("samples.bakehouse.sales_transactions")
b_df_custReview.write.format("delta").mode("overwrite").saveAsTable("agent.bakehouse.media_customer_reviews")

b_df_reviews.write.format("delta").mode("overwrite").saveAsTable("agent.bakehouse.media_gold_reviews_chunked")
b_df_customers.write.format("delta").mode("overwrite").saveAsTable("agent.bakehouse.sales_customers")
b_df_franchises.write.format("delta").mode("overwrite").saveAsTable("agent.bakehouse.sales_franchises")
b_df_suppliers.write.format("delta").mode("overwrite").saveAsTable("agent.bakehouse.sales_suppliers")
b_df_transactions.write.format("delta").mode("overwrite").saveAsTable("agent.bakehouse.sales_transactions")

#Enable change data feed for the existing Delta table
spark.sql("""
ALTER TABLE agent.bakehouse.media_gold_reviews_chunked
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql("""
ALTER TABLE agent.bakehouse.sales_customers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql("""
ALTER TABLE agent.bakehouse.sales_franchises
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql("""
ALTER TABLE agent.bakehouse.sales_suppliers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql("""
ALTER TABLE agent.bakehouse.sales_transactions
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql("""
ALTER TABLE agent.bakehouse.media_customer_reviews
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")



-- Steps 
---- Create vector search endpoint first and then run this code to create function or agents to be used in playground
from databricks.vector_search.client import VectorSearchClient
vector_client = VectorSearchClient()
vector_client.create_endpoint(
     name="bakehouse",
     endpoint_type="STANDARD"
 )

media_customer_reviews_vs = vector_client.create_delta_sync_index(
   endpoint_name="bakehouse",
   source_table_name="agent.bakehouse.media_customer_reviews",
   index_name="agent.bakehouse.media_customer_reviews_vs",
   pipeline_type="TRIGGERED",
   primary_key="new_id",
   embedding_source_column="review",
   embedding_model_endpoint_name="databricks-gte-large-en"
  )

  media_gold_reviews_chunked_vs = vector_client.create_delta_sync_index(
   endpoint_name="bakehouse",
   source_table_name="agent.bakehouse.media_gold_reviews_chunked",
   index_name="agent.bakehouse.media_gold_reviews_chunked_vs",
   pipeline_type="TRIGGERED",
   primary_key="chunk_id",
   embedding_source_column="chunked_text",
   embedding_model_endpoint_name="databricks-gte-large-en"
  )


--- Creating functions / agents to traverse the table

--sales_supplier: 

CREATE OR REPLACE FUNCTION agent.bakehouse.sales_suppliers(
  input_supplier_name STRING COMMENT 'Name of the Supplier whose info to look up',
  input_supplier_id STRING COMMENT 'ID of the Supplier whose info to look up'
)
RETURNS STRING
COMMENT "Returns metadata about a particular supplier given the supplier name and/or id, including all the supplier details. The supplier ID can be used for other queries."
RETURN SELECT CONCAT(
    'Supplier ID: ', supplierID , ', ',
    'Supplier Name: ', name , ', ',
    'Supplier Name: ', ingredient , ', ',
    'Supplier Name: ', continent  , ', ',
    'Supplier city: ', city  , ', ',
    'Supplier district: ', district  , ', ',
    'Supplier size: ', size  , ', ',
    'Supplier longitude: ', longitude  , ', ',
    'Supplier latitude: ', latitude  , ', ',
    'Supplier approved: ', approved 
  )
  FROM agent.bakehouse.sales_suppliers
  WHERE lower(name) = lower(input_supplier_name)
      OR supplierID = input_supplier_id
  LIMIT 1;


--sales_franchise:

CREATE OR REPLACE FUNCTION agent.bakehouse.sales_franchises(
  input_franchise_name STRING COMMENT 'Name of the franchise whose info to look up',
  input_franchise_ID INT COMMENT 'Name of the franchise ID whose info to look up'
)
RETURNS STRING
COMMENT "Returns metadata about a particular franchise given the franchise name and/or id, including all the franchise details. The Franchise ID and SupplierID can be used for other queries."
RETURN SELECT CONCAT(
    'Franchise ID: ', franchiseID , ', ',
    'Franchise name: ', name , ', ',
    'Franchise city: ', city , ', ',
    'Franchise district: ', district  , ', ',
    'Franchise zipcode: ', zipcode  , ', ',
    'Franchise country: ', country  , ', ',
    'Franchise size', size  , ', ',
    'Franchise longitude', longitude  , ', ',
    'Franchise latitude', latitude  , ', ',
    'Supplier ID ', supplierID 

  )
  FROM agent.bakehouse.sales_franchises
  WHERE lower(name) = lower(input_franchise_name)
      OR franchiseID = input_franchise_ID
  LIMIT 1;


--sales_customer: 
CREATE OR REPLACE FUNCTION agent.bakehouse.sales_customers(
  input_first_name STRING COMMENT 'First name of the customer whose info to look up, for instance if user query is "Get me the details of Kayla Barrett", then first name is "Kayla" and not "Kayla Barrett"',
  input_last_name STRING COMMENT 'Last name of the customer whose info to look up,for instance if user query is "Get me the details of Kayla Barrett", then last name is "Barrett" and not "Kayla Barrett"',
  input_customer_ID INT COMMENT 'ID of the customer whose info to look up, for instance if user query is "Get me the details of customer id 2000259", then customer id is 2000259'

)
RETURNS STRING
COMMENT "Returns metadata about a particular customer given the customer name and/or customer id , including all the customer's details. The customer ID can be used for other queries."
RETURN SELECT CONCAT(
    'Customer ID: ', customerID , ', ',
    'Customer First Name: ', first_name  , ', ',
    'Customer Last Name: ', last_name  , ', ',
    'Customer Email address: ', email_address    , ', ',
    'Customer Phone Number: ', phone_number    , ', ',
    'Customer Address: ', address    , ', ',
    'Customer City: ', city   , ', ',
    'Customer State: ', state   , ', ',
    'Customer Country: ', country   , ', ',
    'Customer Continent: ', continent   , ', ',
    'Customer Postal Zip Code: ', postal_zip_code   , ', ',
    'Customer Gender: ', gender 
  )
  FROM agent.bakehouse.sales_customers
  WHERE (lower(first_name) = lower(input_first_name) 
    AND lower(last_name) = lower(input_last_name))
    OR customerID = input_customer_ID
  LIMIT 1;


--sales_transactions: 

  CREATE OR REPLACE FUNCTION agent.bakehouse.sales_transactions(
  input_product_name STRING COMMENT 'Name of the product whose info to look up',
  input_transaction_id INT COMMENT 'Id of the transaction whose info to look up'
)
RETURNS STRING
COMMENT "Returns metadata about a particular customer given the customer name, including the customer's email, ID and last viewed item. The customer ID can be used for other queries."
RETURN SELECT CONCAT(
    'Transaction ID: ', transactionID , ', ',
    'Customer ID: ', customerID , ', ',
    'Franchise ID: ', franchiseID , ', ',
    'Transaction Datetime: ', dateTime , ', ',
    'Product: ', product , ', ',
    'Quantity: ', quantity , ', ',
    'Unit Price: ', unitPrice , ', ',
    'Total Price: ', totalPrice , ', ',
    'Payment Metod: ', paymentMethod , ', ',
    'Card Numer: ', cardNumber 
  )
  FROM agent.bakehouse.sales_transactions
  WHERE lower(product) = lower(input_product_name)
    OR transactionID = input_transaction_id
  LIMIT 1;


--media_customer_review: 

CREATE OR REPLACE FUNCTION agent.bakehouse.media_customer_review_similarity_search (
  query STRING
  COMMENT 'The query string for searching media customer review.'
) RETURNS TABLE
COMMENT 'Executes a search on media customer review documents to retrieve text documents most relevant to the input query.'
RETURN
SELECT
  review as page_content,
  new_id as metadata
FROM
  vector_search(
    index => 'agent.bakehouse.media_customer_reviews_vs',
    query => query,
    num_results => 5
  );

--media_gold_reviews_chunked: 

  CREATE OR REPLACE FUNCTION agent.bakehouse.media_gold_reviews_chunked_similarity_search (
  query STRING
  COMMENT 'The query string for searching media gold reviews chunked documents.'
) RETURNS TABLE
COMMENT 'Executes a search on media gold reviews chunked documents to retrieve text documents most relevant to the input query.'
RETURN
SELECT
  chunked_text as page_content,
  chunk_id as metadata
FROM
  vector_search(
    index => 'agent.bakehouse.media_gold_reviews_chunked_vs',
    query => query,
    num_results => 5
  );