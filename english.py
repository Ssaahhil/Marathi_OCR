import os
import pdfplumber
import pandas as pd
import re
import pyodbc

# === Config ===
pdf_folder = r"C:\Users\ORNET91\Downloads\Yadi Details 2025\Yadi Details 2025\Addition List\Addition List - 188"
excel_output = "Addition_143.xlsx"
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=WORKSTATION4;DATABASE=Supplement_Data;UID=sa;PWD=work$pace@04"

# === Helper: Split Full Name into first, middle, last ===
def split_name(full_name):
    parts = full_name.strip().split()
    first = parts[0] if len(parts) > 0 else ""
    middle = " ".join(parts[1:-1]) if len(parts) > 2 else ""
    last = parts[-1] if len(parts) > 1 else ""
    return full_name, first, middle, last

# === Helper: Extract header info (Status, Month, Year) ===
def extract_header_info(text):
    status = ""
    month = ""
    year = ""

    if re.search(r'Addition', text, re.I):
        status = "A"
    elif re.search(r'Deletion', text, re.I):
        status = "D"
    elif re.search(r'Modification', text, re.I):
        status = "M"

    match = re.search(r'\b(\d{1,2})-(20\d{2})\b', text)
    if match:
        month, year = match.groups()

    return status, month, year

# === Main Extraction ===
all_records = []
pdf_files = [os.path.join(pdf_folder, f) for f in os.listdir(pdf_folder) if f.lower().endswith(".pdf")]

for file_index, pdf_path in enumerate(pdf_files, start=1):
    print(f"\n📂 Processing file {file_index}/{len(pdf_files)}: {os.path.basename(pdf_path)}")
    file_rows = 0

    with pdfplumber.open(pdf_path) as pdf:
        header_text = pdf.pages[0].extract_text() or ""
        status, month, year = extract_header_info(header_text)

        for page_num, page in enumerate(pdf.pages, start=1):
            table = page.extract_table()
            if not table:
                continue

            # Skip first row on every page (header)
            headers = table[0]
            rows = table[1:]

            # Detect Full Name and EPIC dynamically
            header_lower = [str(h).strip().lower() for h in headers]
            full_name_candidates = ["full name", "fullname", "name", "voter name"]
            epic_candidates = ["epic", "idcard", "id card", "voter id"]

            full_name_idx = next((i for i, h in enumerate(header_lower) if h in full_name_candidates), 4)
            epic_idx = next((i for i, h in enumerate(header_lower) if h in epic_candidates), 5)

            for row in rows:
                if not any(row):
                    continue
                try:
                    state = row[0]
                    district = row[1]
                    ac_no = row[2]
                    part_no = row[3]

                    # Full Name
                    full_name = row[full_name_idx] if full_name_idx is not None else ""
                    full_name = " ".join(str(full_name).split()).title() if full_name else ""
                    full_name, first_name, middle_name, last_name = split_name(full_name)

                    # EPIC / Idcard_No
                    idcard_no = row[epic_idx] if epic_idx is not None else ""

                    all_records.append([
                        state, district, ac_no, part_no,
                        idcard_no, full_name, first_name, middle_name, last_name,
                        month, year, status
                    ])
                    file_rows += 1
                except Exception as e:
                    print(f"⚠ Error parsing row on page {page_num}: {e}")
                    continue

    print(f"   ✅ Extracted {file_rows} rows from {len(pdf.pages)} pages")

# === Build DataFrame ===
columns = ["State", "District", "Ac_No", "List_No", "Idcard_No",
           "Fullname", "Name", "Middlename", "Surname",
           "Month", "Year", "Status_Type"]

df = pd.DataFrame(all_records, columns=columns)

# === Save to Excel ===
df.to_excel(excel_output, index=False)
print(f"\n📊 Saved {len(df)} total rows into {excel_output}")

# === Insert into SQL ===
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for _, row in df.iterrows():   #Addition  ,  Deleted, Modification
        cursor.execute("""
            INSERT INTO Addition ([State],[District],[Ac_No],[List_No],[Idcard_No],[Fullname],
                                        [Name],[Middlename],[Surname],
                                        [Month],[Year],[Status_Type])
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {len(df)} rows into SQL table successfully!")

except Exception as e:
    print(f"❌ SQL Insert Error: {e}")
