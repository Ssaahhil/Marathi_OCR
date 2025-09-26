import pandas as pd
from sqlalchemy import create_engine
import urllib

# Update with your actual SQL Server details
server = 'WORKSTATION4'                 # Or 'SERVER\\INSTANCE'
database = 'Supplement_Data'
username = 'sa'
password = 'work$pace@04'

# Connection string
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Load Excel
df = pd.read_excel('D:\\Sahil_Tejam\\ALL_OCR\\Marathi_OCR\\141_Data\\Modification_141.xlsx')  # Replace with actual path

# Insert into your existing 'Addition' table
df.to_sql('Modification', con=engine, if_exists='append', index=False)

print("✅ Data inserted successfully into 'M' table.")

# # === Step 2: Create SQL Server Connection ===
# params = urllib.parse.quote_plus(
#     "DRIVER={ODBC Driver 17 for SQL Server};"
#     "SERVER=WORKSTATION4;"           # 🔁 e.g., localhost, SERVER\\SQLEXPRESS
#     "DATABASE=Supplement_Data;"       # 🔁 Replace with your DB name
#     "UID=sa;"                 # 🔁 Or remove if using trusted connection
#     "PWD=work$pace@04;"                 # 🔁
#     "Trusted_Connection=yes;"             # ➕ Set to 'yes' for Windows Auth
# )

# engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# # === Step 3: Insert into 'Addition' table ===
# df.to_sql('Addition', con=engine, if_exists='append', index=False)

# print("✅ Data inserted successfully into 'Addition' table.")
