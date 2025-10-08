import fitz  # PyMuPDF
import pytesseract
from PIL import Image, ImageEnhance
import pandas as pd
import cv2
import numpy as np
import re
import os
# Optional: Set path to Tesseract manually
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
desired_order = ['file_name', 'ward_no', 'ulb_name', 'पुरुष', 'स्त्री', 'इतर', 'एकूण_निव्वळ_मतदार']

# ---- Image Preprocessing ----
def preprocess_image_sharp(image):
    img_array = np.array(image)
    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    return Image.fromarray(gray)

def enhance_image_quality(image):
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2.0)
    enhancer = ImageEnhance.Sharpness(image)
    image = enhancer.enhance(2.0)
    return image

# ---- Extract image from PDF ----
def extract_table_region(pdf_path, page_num, zoom_factor=3.0):
    doc = fitz.open(pdf_path)
    page = doc[page_num]
    mat = fitz.Matrix(zoom_factor, zoom_factor)
    pix = page.get_pixmap(matrix=mat, dpi=300)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    doc.close()
    return img

# ---- Fixed-pixel cropping ----
def crop_table_area(image, top=1500, bottom=2000, left=100, right_offset=100):
    width, height = image.size
    right = width - right_offset
    if bottom <= top:
        raise ValueError(f"Invalid crop area: bottom ({bottom}) must be greater than top ({top})")
    cropped = image.crop((left, top, right, bottom))
    return cropped

# ---- OCR + Enhancement ----
def extract_text_with_layout(image, lang='mar'):
    # enhanced = enhance_image_quality(image)
    # processed_img = preprocess_image_sharp(enhanced)
    # processed_img.save("debug_sharp_processed.png")
    # print("✅ Saved sharp processed image")
    config = r'--oem 3 --psm 6 -c preserve_interword_spaces=1'
    text = pytesseract.image_to_string(image, lang=lang, config=config)
    return text , image#, processed_img

# ---- Marathi to English digit conversion ----
def convert_marathi_digits(text):
    marathi_to_latin = str.maketrans('०१२३४५६७८९', '0123456789')
    return text.translate(marathi_to_latin)

# ---- Filename Parsing ----
def extract_ward_and_ulb(base_name):
    # Your existing function to extract ward_no and ulb_name
    # Example implementation:
    # base_name: "DraftList_Ward_28_KDMC"
    ward_no = None
    ulb_name = None

    # Extract ward number
    import re
    ward_match = re.search(r'Ward_(\d+)', base_name)
    if ward_match:
        ward_no = ward_match.group(1)

    # Extract ulb name (assuming last part after last underscore)
    parts = base_name.split('_')
    if parts:
        ulb_name = parts[-1].lower()

    return ward_no, ulb_name



# ---- Table Parsing ----
def parse_table_from_text(text):
    lines = text.strip().split('\n')
    table_rows = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        cells = re.split(r'\s{2,}|\t+', line)
        cells = [c.strip() for c in cells if c.strip()]
        if cells:
            table_rows.append(cells)
    return table_rows

# ---- DataFrame Creation ----
def create_clean_dataframe(table_rows):
    if len(table_rows) < 3:
        print("⚠️ Not enough rows to form a table (found only", len(table_rows), ")")
        return pd.DataFrame()

    print("\n📌 Detected Table Rows:")
    for row in table_rows:
        print(row)

    header_row_1 = table_rows[1]
    header_row_2 = table_rows[2]

    # Pad to equal length
    max_len = max(len(header_row_1), len(header_row_2))
    header_row_1 += [""] * (max_len - len(header_row_1))
    header_row_2 += [""] * (max_len - len(header_row_2))

    full_headers = [
        f"{h1.strip()} {h2.strip()}".strip()
        for h1, h2 in zip(header_row_1, header_row_2)
    ]

    data_rows = table_rows[3:]
    valid_data_rows = []
    for i, row in enumerate(data_rows):
        if len(row) == len(full_headers):
            valid_data_rows.append(row)
        else:
            print(f"⚠️ Skipping row {i + 3}: Expected {len(full_headers)} columns, got {len(row)}: {row}")

    if not valid_data_rows:
        print("⚠️ No valid data rows matched the header structure.")
        return pd.DataFrame()

    df = pd.DataFrame(valid_data_rows, columns=full_headers)
    df.columns = [col.replace(' ', '_') for col in df.columns]
    return df

# ---- Voter Count Extraction ----
def extract_voter_counts(text):
    text = convert_marathi_digits(text.replace(',', ''))
    pattern = r"एकूण\s+निव्वळ\s+मतदार\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)"
    match = re.search(pattern, text)
    if match:
        male = int(match.group(1))
        female = int(match.group(2))
        other = int(match.group(3))
        total = int(match.group(4))
        print("✅ Extracted voter counts successfully")
        return {
            'पुरुष': male,
            'स्त्री': female,
            'इतर': other,
            'एकूण_निव्वळ_मतदार': total
        }
    else:
        print("❌ Could not match voter counts from text.")
        return {}

# ---- Wrapper ----
def extract_specific_table_data(text):
    return extract_voter_counts(text)

# ---- Full PDF Extract Flow ----
def extract_table_from_pdf(pdf_path, page_num=0, save_debug=True):
    print(f"\n📄 Processing page {page_num + 1}...")
    full_image = extract_table_region(pdf_path, page_num, zoom_factor=3.0)
    if save_debug:
        full_image.save(f"debug_full_page_{page_num + 1}.png")

    print("✂️ Cropping table region...")
    table_image = crop_table_area(full_image, top=1500, bottom=2000, left=100, right_offset=100)
    if save_debug:
        table_image.save(f"debug_table_region_{page_num + 1}.png")

    print("🔍 Running OCR...")
    text, processed_img = extract_text_with_layout(table_image)
    text = convert_marathi_digits(text)

    print("\n📝 Extracted Text:\n", text)
    print("\n📊 Parsing table structure...")
    table_rows = parse_table_from_text(text)
    specific_data = extract_specific_table_data(text)
    return text, table_rows, specific_data

if __name__ == "__main__":
    folder_path = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\testing_pdfs"
    output_txt = "extracted_marathi_text_combined.txt"
    output_excel = "extracted_table_clean_combined.xlsx"

    all_table_dfs = []
    all_voter_dicts = []

    for file in os.listdir(folder_path):
        if file.endswith('.pdf'):
            pdf_path = os.path.join(folder_path, file)
            file_name = os.path.basename(pdf_path)
            base_name = os.path.splitext(file_name)[0]

            ward_no, ulb_name = extract_ward_and_ulb(base_name)
            print(f"\nProcessing file: {file_name}")
            print(f"Extracted ward_no: {ward_no}")
            print(f"Extracted ulb_name: {ulb_name}")

            text, table_rows, specific_data = extract_table_from_pdf(
                pdf_path,
                page_num=0,
                save_debug=True
            )

            # Save all extracted text to one combined file (optional)
            with open(output_txt, 'a', encoding='utf-8') as f:  # 'a' for append
                f.write(f"\n\n--- {file_name} ---\n")
                f.write(text)

            df = create_clean_dataframe(table_rows)

            if not df.empty:
                df['file_name'] = file_name
                df['ward_no'] = ward_no
                df['ulb_name'] = ulb_name
                # Reorder columns
                desired_order = ['file_name', 'ward_no', 'ulb_name'] + [col for col in df.columns if col not in ['file_name', 'ward_no', 'ulb_name']]
                df = df[desired_order]
                all_table_dfs.append(df)

            if specific_data:
                specific_data['file_name'] = file_name
                specific_data['ward_no'] = ward_no
                specific_data['ulb_name'] = ulb_name
                all_voter_dicts.append(specific_data)

    # Combine all tables
    if all_table_dfs:
        combined_df = pd.concat(all_table_dfs, ignore_index=True)
    else:
        combined_df = pd.DataFrame()

    # Combine all voter data dicts into a DataFrame
    if all_voter_dicts:
        voter_df = pd.DataFrame(all_voter_dicts)
        desired_order_counts = ['file_name', 'ward_no', 'ulb_name', 'पुरुष', 'स्त्री', 'इतर', 'एकूण_निव्वळ_मतदार']
        desired_order_counts = [col for col in desired_order_counts if col in voter_df.columns]
        voter_df = voter_df[desired_order_counts]
    else:
        voter_df = pd.DataFrame()

    # Save combined dataframes to Excel
    if not combined_df.empty or not voter_df.empty:
        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            if not combined_df.empty:
                combined_df.to_excel(writer, sheet_name='Table_Data', index=False)
                print(f"✅ Combined Table_Data saved.")
            if not voter_df.empty:
                voter_df.to_excel(writer, sheet_name='Voter_Counts', index=False)
                print(f"✅ Combined Voter_Counts saved.")
        print(f"📁 Saved combined Excel to: {output_excel}")
    else:
        print("⚠️ No data to save to Excel.")

