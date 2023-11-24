import pandas as pd

# Snowflake connector libraries
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

def print_success():
  print("added successfully")

print_success()

#ConnectionStringModule
def create_connection():
   conn = snow.connect(user="ZAKSNOW002",
   password="zakSNOW:;234",
   account="wwmzytk-td30658",
   warehouse="COMPUTE_WH",
   database="PROJ30DB",
   schema="PROJ30DEV")
   cursor = conn.cursor()
   print('SQL Connection Created')
   return cursor,conn

#TruncateTableModuleIfExists
def truncate_table():
   cur,conn=create_connection()
   sql_titles = "TRUNCATE TABLE IF EXISTS TITLES_RAW"
   sql_credits = "TRUNCATE TABLE IF EXISTS CREDITS_RAW"
   cur.execute(sql_titles)
   cur.execute(sql_credits)
   print('Tables truncated')

#DynamicallyCreateAndLoadDatainSnowflake
def load_data():
   cur,conn=create_connection()
   titles_file = r"/Users/mac/Library/CloudStorage/OneDrive-Personal/Data Engineer Programs/ProjectPro/dbt fundamentals/Code/datasets/titles.csv" # <- Replace with your path.
   titles_delimiter = "," # Replace if you're using a different delimiter.
   credits_file=r"/Users/mac/Library/CloudStorage/OneDrive-Personal/Data Engineer Programs/ProjectPro/dbt fundamentals/Code/datasets/credits.csv"
   credits_delimiter=","

   titles_df = pd.read_csv(titles_file, sep = titles_delimiter)
   print("Titles file read")
   credits_df = pd.read_csv(credits_file, sep = titles_delimiter)
   print("Credits file read")

   write_pandas(conn, titles_df, "TITLES",auto_create_table=True)
   print('Titles file loaded')
   write_pandas(conn, credits_df, "CREDITS",auto_create_table=True)
   print('Credits file loaded')

   cur = conn.cursor()


   #endconnection
   cur.close()
   conn.close()

print("Starting Script")
truncate_table()
load_data()



