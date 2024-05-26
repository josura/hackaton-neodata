# spark sql API for python
from pyspark.sql import SparkSession

# xml parsing
import xml.etree.ElementTree as ET

# xml to pandas
from xml_clinic_utils import xml2dfUnion as xml2df

# pandas
import pandas as pd


# google generative AI API
import google.generativeai as genai
from dotenv import load_dotenv
import os

load_dotenv()

# Configure the API key

genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))
# The Gemini 1.5 models are versatile and work with both text-only and multimodal prompts
model = genai.GenerativeModel('gemini-1.5-flash')

for m in genai.list_models():
  if 'generateContent' in m.supported_generation_methods:
    print(m.name)


# response = model.generate_content("Write a story about a magic backpack.")
# print(response.text)

# Path to XML files
#TODO generalize the file loading
xml_file_2021 = "data/19092400-FlussoEMUR_2021-01-01_2021-12-31_crypt.xml"
xml_file_2022 = "data/19092400-FlussoEMUR_2022-01-01_2022-12-31_crypt.xml"
xml_file_2023 = "data/19092400-FlussoEMUR_2023-01-01_2023-12-31_crypt.xml"

# Parse the XML file
tree_2021 = ET.parse(xml_file_2021)
tree_2022 = ET.parse(xml_file_2022)
tree_2023 = ET.parse(xml_file_2023)

# Get the root element
root_2021 = tree_2021.getroot()
root_2022 = tree_2022.getroot()
root_2023 = tree_2023.getroot()


# Convert the XML data to a pandas DataFrame
df_2021 = xml2df(root_2021)
df_2022 = xml2df(root_2022)
df_2023 = xml2df(root_2023)

# remove all null columns
df_2021 = df_2021.dropna(axis=1, how='all')
df_2022 = df_2022.dropna(axis=1, how='all')
df_2023 = df_2023.dropna(axis=1, how='all')

# convert the columns to string
df_2021 = df_2021.astype(str)
df_2022 = df_2022.astype(str)
df_2023 = df_2023.astype(str)


# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Read the dataframe into a Spark DataFrame
spark_df_2021 = spark.createDataFrame(df_2021, schema = df_2021.columns.to_list())
spark_df_2022 = spark.createDataFrame(df_2022, schema = df_2022.columns.to_list())
spark_df_2023 = spark.createDataFrame(df_2023, schema = df_2023.columns.to_list())

# create a temporary view
spark_df_2021.createOrReplaceTempView("patients2021")
spark_df_2022.createOrReplaceTempView("patients2022")
spark_df_2023.createOrReplaceTempView("patients2023")


# cycle to take inputs from the user
while True:
    print("Available views are:")
    print(spark.catalog.listTables())
    print("The schemas of the data for the tables are: ")
    # TODO generalize the printing
    print(spark_df_2021.columns)
    print(spark_df_2022.columns)
    print(spark_df_2023.columns)
    user_input = input("Enter what you want from the data (type exit to stop the program), this will create an sql query to search for the data: ")
    if user_input == "exit":
        break
    else:
        prompt = "create a spark sql query to get the data, the available tables are" + str(spark.catalog.listTables())
        # add the schemas of the views to the prompt
        # TODO generalize
        prompt += "The schemas of the data for the tables are: "
        prompt += "patients2021 : " + str(spark_df_2021.columns)
        prompt += "patients2022 : " + str(spark_df_2022.columns)
        prompt += "patients2023 : " + str(spark_df_2023.columns)
        response = model.generate_content(user_input + prompt)

        # filter the sql query (localized by ```sql <sql query>```)
        sql_query = response.text[response.text.find("sql") + 4:response.text.find("```", response.text.find("sql"))]
        print("The SQL query given is: ")
        print(sql_query)

        if len(sql_query) == 0:
            print("No SQL query found")
            continue

        # TODO add error handling for the sql query
        # TODO add sanitization for the sql query
        
        spark.sql(sql_query).show()


    

