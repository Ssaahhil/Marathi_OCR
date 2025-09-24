import pdfplumber
import pandas as pd
import re
import os

# === Config ===
pdf_folder = r"C:\Users\ORNET91\Downloads\Yadi Details 2025\Yadi Details 2025\Updation Yadi\141"
excel_output = "Modification_141.xlsx"

# You can set this manually (same value will apply to all rows)
status_type = "M"

# Month mapping
month_map = {
    "January": "01", "February": "02", "March": "03", "April": "04",
    "May": "05", "June": "06", "July": "07", "August": "08",
    "September": "09", "October": "10", "November": "11", "December": "12"
}

def clean_name(name):
    """Clean and split full name into First, Middle, Last."""
    # Normalize spaces and apply title case
    name = " ".join(name.strip().split())
    name = name.title()

    parts = name.split()

    if len(parts) == 1:
        return name, parts[0], "", ""
    elif len(parts) == 2:
        return name, parts[0], "", parts[1]
    elif len(parts) == 3:
        return name, parts[0], parts[1], parts[2]
    else:
        # First + Last, all others as Middle
        return name, parts[0], " ".join(parts[1:-1]), parts[-1]

all_data = []

# === Loop through all PDFs ===
for file in os.listdir(pdf_folder):
    if file.lower().endswith(".pdf"):
        pdf_path = os.path.join(pdf_folder, file)

        # Try matching "2025_April" or "April 2025"
        match1 = re.search(r"(\d{4})[_ ]([A-Za-z]+)", file)
        match2 = re.search(r"([A-Za-z]+)[_ ](\d{4})", file)

        year, month_name = None, None
        if match1:
            year, month_name = match1.groups()
        elif match2:
            month_name, year = match2.groups()

        if not year or not month_name:
            print(f"⚠️ Skipping {file} (No Year_Month found in name)")
            continue

        month_num = month_map.get(month_name, "00")

        # Open PDF
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    headers = table[0]
                    for row in table[1:]:
                        try:
                            # Skip Serial No (col index 0)
                            state, district, ac_no, part_no, epic, fullname = row[1:]

                            # Clean and split name
                            fullname, first_name, middle_name, last_name = clean_name(fullname)

                            all_data.append([
                                state, district, ac_no, part_no, epic,
                                fullname, first_name, middle_name, last_name,
                                month_num, year, status_type
                            ])
                        except Exception as e:
                            print(f"⚠️ Error in {file}, row skipped: {e}")

# === Create DataFrame ===
df = pd.DataFrame(all_data, columns=[
    "State", "District", "AC_No", "List_No", "EPIC",
    "Full_Name", "First_Name", "Middle_Name", "Last_Name",
    "Month", "Year", "Status_Type"
])

# Save to Excel
df.to_excel(excel_output, index=False)

print("✅ Extraction completed. Data saved to", excel_output)
