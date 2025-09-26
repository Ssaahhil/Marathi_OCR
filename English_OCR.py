import fitz
from paddleocr import PaddleOCR
import logging
import os, time, json
import pandas as pd
import re
from PIL import Image
import cv2
from sqlalchemy import create_engine,text,types
import numpy as np
import pytesseract
import pyodbc

logging.getLogger("ppocr").setLevel(logging.WARNING)


# --------------------------------------------
# =========== SQL Serve DB Config =============
# --------------------------------------------
DB_SERVER = "ORNET96"
DB_DRIVER = "ODBC Driver 17 for SQL Server"

DB_USER = "sa"                  # SQL Server username
DB_PASS = "manager"    # SQL Server password
DB_NAME = "KDMC"                # Default database (can be overridden)
TABLE_NAME = "Ward_Unknown" 

# Build connection string (ODBC)
connection_string = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_SERVER}/{DB_NAME}"
    f"?driver={DB_DRIVER.replace(' ', '+')}"
)

# Create SQLAlchemy engine with fast_executemany (better for bulk inserts)
engine = create_engine(connection_string, fast_executemany=True)


# ============== CONFIG(WORKSTATION2) ================
# pdf_folder = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\pdf_extract"
# temp_excel = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\Output\output_temp1.xlsx"
# output_excel = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\Output\tmc_details.xlsx"
# card_image_folder = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\Card_Images"
# os.makedirs(card_image_folder, exist_ok=True)


pdf_folder = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Input_Pdf"
temp_excel = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Output_Sample\output_temp1.xlsx"
output_excel = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Output_Sample\process_test1.xlsx"
card_image_folder = r"Extracted_Card_Img"
os.makedirs(card_image_folder, exist_ok=True)

zoom_factor = 3
# -------------------------------------------
# =========== Tesseract OCR Setup ===========
# --------------------------------------------
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
tesseract_config = "--oem 1 --psm 11 mar"


# -------------------------------------------
# =========== Paddle OCR Setup ===========
# --------------------------------------------
ocr_paddle = PaddleOCR(use_angle_cls=True, lang='en', rec=True, gpu=True, precision='fp16', use_mp=True)


prefix_mapping_file = r"D:\Sahil_Tejam\ALL_OCR\HINDI_OCR\Prefix.xlsx"
sheet_name = "Dombivali-143"
valid_prefixes = set()

try:
    xls = pd.ExcelFile(prefix_mapping_file)
    if sheet_name in xls.sheet_names:
        prefix_df = pd.read_excel(prefix_mapping_file, sheet_name=sheet_name, dtype={"FirstThreeLetters": str})
        prefix_df["cnt"] = pd.to_numeric(prefix_df["cnt"], errors="coerce").fillna(0).astype(int)
        valid_prefixes = set(prefix_df["FirstThreeLetters"].astype(str))
except Exception as e:
    print(f"Error loading prefix mapping file: {e}")


# -------------------------------------------
# =========== DEBUG LOG Setup ===========
# --------------------------------------------
DEBUG = False
LOG_FILE = "debug.log"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("=== OCR Debug Log ===\n")

def debug_log(msg):
    if DEBUG:
        print(msg)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")


# -------------------------------------------------------------
# ========= Precompiled regex for Epic Number patterns =========
# -------------------------------------------------------------
EPIC_SLASH_PATTERN   = re.compile(r"^[A-Z]{2}/\d{2,}/\d{2,}/\d+$", re.IGNORECASE)
EPIC_3L7D_PATTERN    = re.compile(r"^[A-Z]{3}\d{7}$", re.IGNORECASE)
EPIC_2L8D_PATTERN    = re.compile(r"^[A-Z]{2}\d{8}$", re.IGNORECASE)
EPIC_PREFIX_DIGITS   = re.compile(r"^([A-Z]{2,4})(\d{7,8})$")
NON_ALNUM_SLASH      = re.compile(r"[^A-Za-z0-9/]")  # cleanup


# ------------------------------------------------------------------------------
# ========= Precompiled regex for assembly/list/serial numbers patterns =========
# ------------------------------------------------------------------------------
AC_NO_SEARCH     = re.compile(r"\b(\d{1,3})/\d{1,6}/\d{1,6}\b")
LIST_NO_SEARCH   = re.compile(r"\d{1,3}/(\d{1,5})/\d{1,5}\b")
SERIAL_NO_SEARCH = re.compile(r"\d{1,3}/\d{1,5}/(\d{1,5})\b")
LEADING_DIGITS   = re.compile(r"^(\d+)")
NON_DIGITS_SLASH     = re.compile(r"[^0-9/]")  # cleanup


AGE_PATTERN          = re.compile(r"(?:वय|बय|वयं)[:;?\s]*([0-9०-९<]+)")
NON_DIGIT_PATTERN    = re.compile(r"[^0-9०-९]")  # remove junk in age


# -------------------------------------------------------------
# ========= Precompiled regex for Gender patterns =========
# -------------------------------------------------------------
AGE_PREFIX_PATTERN   = re.compile(r"वय\s*[:;?\-]?\s*[^\s\n\r]*")
GENDER_PATTERN       = re.compile(r"(?:लिंग|ळिंग|लिग|छिंग|ठिंग)\s*[:\-]?\s*([^\s\n\r:;]+)", re.IGNORECASE)
HOUSE_PREFIX_PATTERN = re.compile(r"(?:घर\s*क्रमांक|घर\s*क्र\.?)\s*[^\s\n\r]*")

# Predefine known gender tokens
MALE_WORDS   = {"पु"}
FEMALE_WORDS = {
    "स्री", "स्त्री", "सरी", "झरी", "ख्री", "खरी",
    "ख्तरी", "ख्त्री", "खत्री", "खतरी", "सत्री",
    "खसत्री", "खस्तरी", "ख्त्रा", "स््री"
}
OTHER_WORDS  = {"इतर", "ईतर", "इ्तर"}

# ------------------------------------------------------------
# ============ Marathi to English Number Conversion ==========
# ------------------------------------------------------------
def marathi_to_english_number(text):
    marathi_digits = "०१२३४५६७८९"
    english_digits = "0123456789"
    return text.translate(str.maketrans(marathi_digits, english_digits))

def preprocess_image(img: Image.Image,upscale_factor=3) -> Image.Image:
    new_size = (img.width * upscale_factor, img.height * upscale_factor)
    upscaled_img = img.resize(new_size, Image.Resampling.LANCZOS)
    return upscaled_img


# ------------------------------------------------------------
# ============ Find Voter Card Boxes in Page Image ==========
# ------------------------------------------------------------
def find_card_boxes(pixmap_img, min_w=400, min_h=150, max_w=650, max_h=300, iou_thresh=0.3):
    """
    Detect voter card boxes from page image.
    - Filters duplicate inner/outer contours
    - Keeps only one bounding box per card
    """
    gray = cv2.cvtColor(pixmap_img, cv2.COLOR_RGB2GRAY)
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(blur, 50, 150)

    contours, _ = cv2.findContours(edges, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    raw_boxes = []
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        if min_w < w < max_w and min_h < h < max_h:
            raw_boxes.append((x, y, x + w, y + h))

    # Helper: IoU between two boxes
    def iou(boxA, boxB):
        xA = max(boxA[0], boxB[0])
        yA = max(boxA[1], boxB[1])
        xB = min(boxA[2], boxB[2])
        yB = min(boxA[3], boxB[3])
        interArea = max(0, xB - xA) * max(0, yB - yA)
        areaA = (boxA[2] - boxA[0]) * (boxA[3] - boxA[1])
        areaB = (boxB[2] - boxB[0]) * (boxB[3] - boxB[1])
        unionArea = float(areaA + areaB - interArea)
        return interArea / unionArea if unionArea > 0 else 0

    # Deduplicate: keep only one box per overlapping region
    deduped = []
    for b in sorted(raw_boxes, key=lambda b: (b[1], b[0])):  # scan row-wise
        if all(iou(b, d) < iou_thresh for d in deduped):
            deduped.append(b)

    # Sort again top-to-bottom, then left-to-right
    final_boxes = sorted(deduped, key=lambda b: (b[1] // 250, b[0]))

    return final_boxes


# -----------------------------------------------------------
# =========== Check if Voter Card is Present in Image ========
# ------------------------------------------------------------
def card_is_present(image, min_cards=1):
    """
    Returns True if at least `min_cards` voter cards are detected in the image.
    """
    boxes = find_card_boxes(np.array(image))  # PIL to NumPy for OpenCV
    return len(boxes) >= min_cards


# --------------------------------------------------------------------------
# =============== Municipal Coorporation Extractors ==========================
# --------------------------------------------------------------------------
def extract_municipal(header_text):
    # match = re.search(r"([^\s]+)\s*महानगरपालिका", header_text)
    match = re.search(r"(.+?)\s*महानगरपालिका", header_text)
    municipal = match.group(1).strip() if match else ""
    # debug_log(f"[MUNICIPAL] {municipal}")
    return municipal


# ------------------------------------------------------------------------
# =============== Prabhag No and Name Extractors ==========================
# ------------------------------------------------------------------------
def extract_prabhag_info(text):
    prabhag_no, prabhag_name = "", ""
    prabhag_lines = []

    lines = text.splitlines()
    collecting = False

    max_additional_lines = 3  # safety max
    additional_lines_collected = 0
    blank_line_count = 0

    voter_data_pattern = re.compile(r"^\d{1,3}(,\d{1,4})?\s")

    # Match: प्रभाग क्र : <digits> - <name start>
    prabhag_pattern = re.compile(
        r"प्रभाग\s*क्र\.?\s*[:\-]?\s*([०-९0-9]+)\s*[-–—]\s*(.*)"
    )

    for idx, line in enumerate(lines):
        line_stripped = line.strip()

        if not collecting:
            match = prabhag_pattern.search(line_stripped)
            if match:
                prabhag_no = marathi_to_english_number(match.group(1))
                first_line = match.group(2).strip()
                if first_line:
                    prabhag_lines.append(first_line)
                collecting = True
            continue

        if collecting:
            # Stop if we reach voter rows
            if voter_data_pattern.match(line_stripped):
                break

            # Stop if a new header like "यादी" or "भाग" starts
            if line_stripped.startswith("यादी") or line_stripped.startswith("भाग"):
                break

            # Stop at blank lines
            if line_stripped == "":
                blank_line_count += 1
            else:
                blank_line_count = 0
                prabhag_lines.append(line_stripped)

            if blank_line_count >= 2:
                break

            additional_lines_collected += 1
            if additional_lines_collected >= max_additional_lines:
                break

    prabhag_name = " ".join(prabhag_lines).strip()
    return prabhag_no, prabhag_name


# ------------------------------------------------------------------------
# =============== Section No and Name Extractors ==========================
# ------------------------------------------------------------------------
def extract_section_info(text):
    section_no = ""
    section_lines = []

    lines = text.splitlines()
    collecting = False

    max_additional_lines = 10  # safety max lines after header
    additional_lines_collected = 0
    blank_line_count = 0

    voter_data_pattern = re.compile(r"^\d{1,3}(,\d{1,4})?\s")
    digit_pattern = re.compile(r"[0-9०-९]")  # ✅ matches English or Marathi digits

    def normalize_section_name(name: str) -> str:
        """Normalize OCR variants of 'NA' into 'NA'."""
        if not name or not name.strip():
            return "NA"

        cleaned = name.strip().lower().replace(" ", "").replace(".", "")

        # ✅ Add all known weird variants
        na_variants = {
            "na", "n/a", "एनए", "nil", "none", "---",
            "1९%", "1५/", "1९/", "1९»", "1९%","1९", "10»", "1»", "1९०"
        }

        if cleaned in na_variants:
            return "NA"

        return name.strip()

    for idx, line in enumerate(lines):
        line_stripped = line.strip()

        if not collecting:
            match = re.search(
                r"यादी\s*भाग\s*क्र\.?\s*[०-९0-9]+\s*[:\-]\s*([०-९0-9]+)\s*-\s*(.*)",
                line_stripped
            )
            if match:
                section_no = marathi_to_english_number(match.group(1))
                first_line = match.group(2).strip()
                if first_line and not digit_pattern.search(first_line):  
                    section_lines.append(first_line)
                collecting = True
            continue

        if collecting:
            if voter_data_pattern.match(line_stripped):
                break
            if digit_pattern.search(line_stripped):
                break

            if line_stripped == "":
                blank_line_count += 1
            else:
                blank_line_count = 0
                section_lines.append(line_stripped)

            if '.' in line_stripped:
                break
            if blank_line_count >= 2:
                break

            additional_lines_collected += 1
            if additional_lines_collected >= max_additional_lines:
                break

    # ✅ If nothing captured, return NA
    if not section_lines:
        return section_no, "NA"

    section_name = " ".join(section_lines).strip()
    section_name = normalize_section_name(section_name)
    return section_no, section_name


# --------------------------------------------------------------
# =============== Extract Header Info from Page Image ============
# --------------------------------------------------------------
def extract_header_info(page_img, top_margin, zoom_factor):
    header_crop = page_img.crop((0, 0, page_img.width, int(top_margin * zoom_factor)))
    header_text = pytesseract.image_to_string(header_crop, config="--psm 6 -l mar").strip()
    # debug_log(f"[HEADER RAW]\n{header_text}")
    municipal = extract_municipal(header_text)
    section_no, section_name = extract_section_info(header_text)
    # booth_name = extract_booth_name(header_text)
    # booth_address = extract_booth_address(header_text)
    prabhag_no, prabhag_name = extract_prabhag_info(header_text)
    return {
        "Municipal_Corporation": municipal,
        "Prabhag_No": prabhag_no,
        "Prabhag_Name": prabhag_name,
        # "Section_No": section_no,
        "Section_No": section_no,
        "Section_Name": section_name,
        # "Booth_Name": booth_name,
        # "Booth_Address": booth_address,
        "Raw_Header_Text": header_text
    }

def extract_pdf_header_info(pdf_file, zoom_factor):
    with fitz.open(pdf_file) as doc:
        first_page = doc[0]
        pix = first_page.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    hdr = extract_header_info(img, top_margin=118.0, zoom_factor=zoom_factor)
    pdf_header_info = {
        "Municipal_Corporation": hdr.get("Municipal_Corporation", ""),
        "Prabhag_No": hdr.get("Prabhag_No", ""),
        "Prabhag_Name": hdr.get("Prabhag_Name", ""),
        "File_Name": os.path.basename(pdf_file)
    }
    return pdf_header_info


# -----------------------------------------------------------------
# =========== Extract Index Number from PaddleOCR Text ============
# ------------------------------------------------------------------
def extract_index_number(paddle_text):
    """
    Extract index number from PaddleOCR text.
    - Supports formats like '143/19/861', '7,281', '208.75.995'
    - Skips alphanumeric lines (EPIC numbers like HTQ1428796)
    - Returns string (digits + / allowed)
    """
    index_number = ""
    
    lines = [line.strip() for line in paddle_text.splitlines() if line.strip()]
    
    for line in lines:
        # Skip if line has any English letters (likely EPIC or junk)
        if re.search(r"[A-Za-z]", line):
            continue

        # Keep digits, / , and . , but remove all other junk
        cleaned = re.sub(r"[^\d/.,]", "", line)

        # Must contain at least one digit
        if not re.search(r"\d", cleaned):
            continue

        # Normalize: remove commas and dots, but keep slashes
        normalized = cleaned.replace(",", "").replace(".", "")

        if re.match(r"^[\d,\.]+$", cleaned):
            normalized = cleaned.replace(",", "").replace(".", "")
            if normalized.isdigit():
                index_number = normalized
                # debug_log(f"[INDEX_NUM] Extracted (plain): {index_number} from {line}")
                print(f"[INDEX_NUM] Extracted (plain): {index_number} from {line}")
                
                return index_number        

        # # Accept only if it looks like a valid index number (digits with optional / separators)
        # if re.match(r"^\d+(?:/\d+)*$", normalized):
        #     index_number = normalized
        #     debug_log(f"[INDEX_NUM] Extracted: {index_number} from line: {line}")
        #     break
        
    return index_number


# -----------------------------------------------------
# =========== Clean Tesseract OCR Noise ============
# -----------------------------------------------------
def clean_tesseract_text(text: str) -> str:
    """
    Remove fixed OCR noise patterns like 'मि४123456' or 'मिए123456' from Tesseract text.
    """

    import re
    # Remove 'मि४' + digits
    text = re.sub(r"\bमि४\d+\b", "", text)

    # Remove 'मिए' + digits
    text = re.sub(r"मिए8॥81016", "", text)
    
    text = re.sub(r"मिए818016","", text)
    # Cleanup extra spaces / blank lines
    text = re.sub(r"[ ]{2,}", " ", text)
    text = re.sub(r"\n\s*\n", "\n", text)

    return text.strip()


# ------------------------------------------------------------
# =========== Extract Age from Tesseract OCR Text ============
# ------------------------------------------------------------
def extract_age(text):
    """Extract age in Marathi and English digits."""
    match = AGE_PATTERN.search(text)
    if not match:
        # print("[AGE] Not found")
        return "", ""

    age_marathi_raw = match.group(1).strip()
    age_marathi_clean = NON_DIGIT_PATTERN.sub("", age_marathi_raw)  # remove < or other junk
    age_english = marathi_to_english_number(age_marathi_clean)

    # print(f"[AGE] Marathi={age_marathi_clean} English={age_english}")
    return age_marathi_clean, age_english


# ---------------------------------------------------------------
# =========== Extract Gender from Tesseract OCR Text ============
# ---------------------------------------------------------------
def extract_gender(text):
    """Extract gender (raw + normalized) from OCR text (optimized)."""

    # Step 1: Find text after 'वय'
    age_match = AGE_PREFIX_PATTERN.search(text)
    if age_match:
        after_age_text = text[age_match.end():]
        gender_match = GENDER_PATTERN.search(after_age_text)

        raw_gender = gender_match.group(1).strip() if gender_match else ""
        normalized_gender = classify_gender(raw_gender)

        if normalized_gender:  # Found via वय→लिंग
            # print(f"[RAW] {raw_gender} → [NORMALIZED] {normalized_gender}")
            return raw_gender, normalized_gender

    # === Fallback: look after 'घर क्रमांक' ===
    house_match = HOUSE_PREFIX_PATTERN.search(text)
    if house_match:
        after_house_text = text[house_match.end():]

        for word in MALE_WORDS | FEMALE_WORDS | OTHER_WORDS:
            if word in after_house_text:
                raw_gender = word
                normalized_gender = classify_gender(raw_gender)
                # print(f"[FALLBACK RAW] {raw_gender} → [NORMALIZED] {normalized_gender}")
                return raw_gender, normalized_gender

    return "", ""


def classify_gender(raw_text: str) -> str:
    """Normalize OCR variants into standard Marathi root words."""
    raw_text = raw_text.strip().lower()

    if any(word in raw_text for word in MALE_WORDS):
        return "पुरुष"
    if any(word in raw_text for word in FEMALE_WORDS):
        return "स्त्री"
    if any(word in raw_text for word in OTHER_WORDS):
        return "इतर"

    return ""


def marathi_to_english_gender(normalized_gender: str) -> str:
    return {"पुरुष": "Male", "स्त्री": "Female", "इतर": "Other"}.get(normalized_gender, "")


# ---------------------------------------------------------------------
# =========== Parse Voter Card Info from Tesseract OCR Text ============
# ---------------------------------------------------------------------
# def parse_voter_card(marathi_text, cleaned_text):
#     age_marathi, age_english = extract_age(cleaned_text)
#     raw_gender, normalized_gender = extract_gender(cleaned_text)
#     gender_english = marathi_to_english_gender(normalized_gender)

#     return {
#         "Age_Marathi": age_marathi,
#         "Age_English": age_english,
#         "Gender_Marathi": normalized_gender,  # normalized Marathi root word
#         "Gender_English": gender_english,     # English category
#     }



# ---------------------------------------------------------------------------
# =========== Extract and Correct EPIC Number from PaddleOCR Text ============
# ---------------------------------------------------------------------------
def extract_epic_number(paddle_text: str) -> str:
    """
    Extract EPIC number from PaddleOCR text.
    Supports:
      - Structured format: MT/03/017/0050016
      - Old format: RSC9492026 (3 letters + 7 digits)
      - New format: HT01523646 (2 letters + 8 digits)
    """
    epic_number = ""
    lines = [NON_ALNUM_SLASH.sub("", line).strip() for line in paddle_text.splitlines()]
    lines = [line for line in lines if line]

    for line in lines:
        if EPIC_SLASH_PATTERN.match(line):
            epic_number = line.upper()
            break
        elif EPIC_3L7D_PATTERN.match(line):
            epic_number = line.upper()
            break
        elif EPIC_2L8D_PATTERN.match(line):
            epic_number = line.upper()
            break

    return epic_number


def correct_epic_number(epic_number: str) -> str:
    """
    Normalize EPIC numbers:
      - Keep slash-format as-is
      - Always return 3-letter prefix + 7-digit number
      - If OCR gives 2 letters/8 digits, try prefix mapping
      - If prefix mapping fails, return None
    """
    if not epic_number:
        return None

    epic_number = epic_number.strip().upper()

    # Case 1: Slash type → keep as-is
    if "/" in epic_number:
        return epic_number

    # Case 2: Match prefix + digits
    m = EPIC_PREFIX_DIGITS.match(epic_number)
    if not m:
        return None  # Invalid format

    prefix, digits = m.group(1), m.group(2)
    digits = digits[-7:]  # always last 7 digits

    if prefix in valid_prefixes and len(prefix) == 3:
        return prefix + digits

    if len(prefix) == 2:
        best = find_best_prefix(prefix)
        return (best + digits) if best else None

    if len(prefix) >= 3:
        best = find_best_prefix(prefix[:2])
        return (best + digits) if best else prefix[:3] + digits

    return None


# --------------------------------------------------------
# =========== Find Closest Valid Prefix for EPIC ============
# --------------------------------------------------------
def find_best_prefix(prefix: str) -> str:
    """Map a 2-letter prefix to a valid 3-letter prefix."""
    prefix_2 = prefix[:2]
    matches = [p for p in valid_prefixes if p.startswith(prefix_2)]
    return sorted(matches, key=lambda x: (len(x), x), reverse=True)[0] if matches else None


# --------------------------------------------------------
# =========== Extract Assembly/List/Serial Numbers ============
# --------------------------------------------------------
def extract_assembly_consitution_no(paddle_text: str) -> str:
    """
    Extract Assembly Constituency No (first part of 188/36/12 -> 188).
    Uses a single regex scan over the text.
    """
    match = AC_NO_SEARCH.search(paddle_text)
    return match.group(1) if match else ""


def extract_list_number(paddle_text: str) -> str:
    """
    Extract List number (middle part of 188/36/12 -> 36).
    """
    match = LIST_NO_SEARCH.search(paddle_text)
    list_number = match.group(1) if match else ""
    if list_number:
        debug_log(f"[LIST] Extracted={list_number}")
    return list_number


def extract_serial_number(paddle_text: str, extracted_text: str, serial_counter: int, ocr_empty: bool):
    """
    Extract Serial number:
    - First tries from fraction style like 188/36/12 (→ last part)
    - If not found, fallback to sequential numbering
    """
    match = SERIAL_NO_SEARCH.search(paddle_text)
    if match:
        serial_number = match.group(1)
        serial_source_text = f"Fraction style: {match.group(0)}"
        return serial_number, serial_source_text, serial_counter

    # Fallback → sequential numbering
    if not ocr_empty and extracted_text.strip():
        serial_match = LEADING_DIGITS.match(extracted_text.strip())
        serial_number = int(serial_match.group(1)) if serial_match else serial_counter
        serial_source_text = serial_match.group(0) if serial_match else "Not Found - Assigned Sequentially"
        serial_counter += 1
    elif not ocr_empty:
        serial_number, serial_source_text = serial_counter, "Not Found - Assigned Sequentially"
        serial_counter += 1
    else:
        serial_number, serial_source_text = None, "Skipped - Empty OCR"

    return serial_number, serial_source_text, serial_counter


# ---------------------------------------------------------------
# ================ File Name Extractor ================
# ---------------------------------------------------------------
def get_file_name(pdf_file):
    """
    Extract only the file name (without extension) from a PDF path.
    Example: C:/folder/Booth9.pdf -> Booth9
    """
    return os.path.splitext(os.path.basename(pdf_file))[0]


# ---------------------------------------------------------------
# ============= Emergency Save Helpers ===============
# ---------------------------------------------------------------
def save_progress(voter_details, column_order, temp_excel):
    """Save partial progress to a temp Excel file."""
    if not voter_details:
        return
    df = pd.DataFrame(voter_details)
    df = df[[col for col in column_order if col in df.columns]]
    df.to_excel(temp_excel, index=False, engine="openpyxl")  # ✅ removed encoding
    print(f"💾 Progress saved in temp file: {temp_excel}")


def finalize_output(temp_excel, output_excel):
    """Finalize output by renaming temp file → final file and removing temp."""
    if os.path.exists(temp_excel):
        os.replace(temp_excel, output_excel)  # atomic replace
        print(f"✅ Final file saved at: {output_excel}")
        try:
            os.remove(temp_excel)  # cleanup (on Windows os.replace already moves it)
            print("🗑️ Temp file deleted")
        except FileNotFoundError:
            pass
    else:
        print("⚠️ Temp file not found. Nothing to finalize.")


column_order = [
    "File_Name","New_Voter_ID","Municipal_Corporation", "Prabhag_No", "Prabhag_Name",
    "Voter_ID", "Section_No", "Section_Name","List_Number","Page",
    "Ac_no","EPIC_Number",
    # "Age_Marathi", "Age_English",
    # "Gender_Marathi", "Gender_English",
    # "Booth_Name", "Booth_Address",
     "Card_Index",
    # "Marathi_Text","Cleaned_Text", 
    "Paddle_Text",
]



# ---------------------------------------------------------------
# ================ Main Page Processing Function ===============
# ---------------------------------------------------------------
def process_page(pdf_file, page_num, zoom_factor, pdf_header_info):
    """
    Process a single page and return voter-level details:
    - EPIC, List No, AC No, Index No, Serial No
    - Plus header info: Section_No, Section_Name, and file metadata
    """
    import fitz  # PyMuPDF
    from PIL import Image
    import numpy as np
    import os

    voter_details = []
    serial_counter = 1

    # === Load PDF & Page ===
    doc = fitz.open(pdf_file)
    page = doc[page_num - 1]
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))
    full_img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    # === Extract Page Header Info ===
    header_info = extract_header_info(full_img, top_margin=118.0, zoom_factor=zoom_factor)
    section_no = header_info.get("Section_No", "")
    section_name = header_info.get("Section_Name", "")

    print(f"📌 Page {page_num} Header → Section_No: {section_no} | Section_Name: {section_name}")

    # === Convert to NumPy Array ===
    pix_np = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if pix_np.shape[2] == 4:
        pix_np = pix_np[:, :, :3]

    # === Detect Voter Card Boxes ===
    card_coords_points = find_card_boxes(pix_np)
    if not card_coords_points:
        print(f"⚠️ No card boxes detected on page {page_num}")
        doc.close()
        return []

    # === OCR Each Voter Card Box ===
    for card_index, (x1, y1, x2, y2) in enumerate(card_coords_points, start=1):
        card_img = full_img.crop((x1, y1, x2, y2))
        preprocessed_img = preprocess_image(card_img)

        result_paddle = ocr_paddle.ocr(np.array(preprocessed_img))
        paddle_text = "\n".join([line[1][0] for line in result_paddle[0]]) if result_paddle and result_paddle[0] else ""

        epic_number = extract_epic_number(paddle_text)
        list_number = extract_list_number(paddle_text)
        ac_no = extract_assembly_consitution_no(paddle_text)
        index_number = extract_index_number(paddle_text)
        serial_number, _, serial_counter = extract_serial_number(
            paddle_text, paddle_text, serial_counter, False
        )

        # === Save Minimal Parsed Info ===
        voter_details.append({
            "EPIC_Number": correct_epic_number(epic_number) if epic_number else None,
            "List_Number": list_number,
            "AC_No": ac_no,
            "Index_Number": index_number,
            "Serial_Number": serial_number,
            "Section_No": section_no,
            "Section_Name": section_name,
            "Page": page_num,
            "Card_Index": card_index,
            "Municipal_Corporation": pdf_header_info.get("Municipal_Corporation", ""),
            "Prabhag_No": pdf_header_info.get("Prabhag_No", ""),
            "Prabhag_Name": pdf_header_info.get("Prabhag_Name", ""),
            "File_Name": pdf_header_info.get("File_Name", os.path.basename(pdf_file)),
        })

    doc.close()
    return voter_details



# ---------------------------------------------------------------
# ================ Checkpointing Helpers ===============    
# ---------------------------------------------------------------
CHECKPOINT_FILE = "checkpoint.json"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_checkpoint(pdf_name, page_num, temp_excel):
    checkpoint = load_checkpoint()
    checkpoint[pdf_name] = {
        "last_page": int(page_num),  # store page as int for JSON
        "temp_excel": temp_excel
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2)
    print(f"💾 Checkpoint saved for {pdf_name} at page {page_num}")
    
    
# ---------------------------------------------------------------
# ================ SQL Server Insertion Helpers ===============
# ---------------------------------------------------------------    

# === Create SQLAlchemy Engine ===
# def get_engine(db_name, user=DB_USER, password=DB_PASS):
#     """
#     Create SQLAlchemy engine using SQL Server Authentication.
#     """
#     conn_str = f"mssql+pyodbc://{user}:{password}@{DB_SERVER}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
#     return create_engine(conn_str, fast_executemany=True)

# # === Ensure Database Exists ===
# def ensure_database_exists(db_name, user=DB_USER, password=DB_PASS):
#     """
#     Creates the database if it doesn't exist using raw pyodbc (autocommit=True).
#     Avoids 'CREATE DATABASE inside transaction' error.
#     """
#     conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};UID={user};PWD={password};DATABASE=master"
#     with pyodbc.connect(conn_str, autocommit=True) as conn:
#         cursor = conn.cursor()
#         cursor.execute(f"""
#             IF NOT EXISTS (SELECT name FROM sys.databases WHERE name='{db_name}')
#                 CREATE DATABASE [{db_name}]
#         """)
#         print(f"✅ Database ready: {db_name}")

# # === Clean + Validate Integer Columns ===
# def enforce_integer_columns(df, int_cols):
#     for col in int_cols:
#         if col in df.columns:
#             numeric_series = pd.to_numeric(df[col], errors="coerce")
#             bad_mask = numeric_series.isna() & df[col].notna()
#             if bad_mask.any():
#                 bad_vals = df[col][bad_mask].unique()
#                 raise ValueError(f"❌ Column '{col}' contains non-integer values: {bad_vals}")
#             if not (numeric_series.dropna() == numeric_series.dropna().astype(int)).all():
#                 bad_vals = df[col][numeric_series != numeric_series.astype(int)].unique()
#                 raise ValueError(f"❌ Column '{col}' contains non-integer decimal values: {bad_vals}")
#             df[col] = numeric_series.astype("Int64")
#     return df

# # === Extract Table Name from Excel/PDF File Name ===
# def extract_table_name(excel_path):
#     """
#     Example: DraftList_Ward_28_KDMC.xlsx -> Ward_28
#     """
#     base = os.path.splitext(os.path.basename(excel_path))[0]
#     ward_match = re.search(r"Ward[_ ]?(\d+)", base, re.IGNORECASE)
#     return f"Ward_{ward_match.group(1)}" if ward_match else "Ward_Unknown"

# # === Insert Excel into SQL Server ===
# def insert_excel_to_sql(excel_path, db_name=DB_NAME, exclude_cols=None):
#     """
#     Reads an Excel file and inserts it into SQL Server.
#     All text columns (Marathi included) are stored as NVARCHAR.
#     Integer columns remain INT.
#     Replaces the table if it already exists.
#     Returns (engine, table_name) for further processing.
#     """
#     try:
#         print(f"📂 Reading Excel file: {excel_path}")
#         df = pd.read_excel(excel_path, dtype=str)

#         if df.empty:
#             print("⚠️ Excel file is empty, nothing to insert.")
#             return None, None

#         if exclude_cols:
#             df = df.drop(columns=exclude_cols, errors="ignore")

#         # Columns that must be integers
#         int_cols = [
#             "New_Voter_ID", "Voter_ID", "Section_No", "List_Number",
#             "Page", "Card_Index", "Prabhag_No", "Ac_no", "Age_English"
#         ]
#         df = enforce_integer_columns(df, int_cols)

#         # Ensure database exists
#         ensure_database_exists(db_name)

#         # Extract table name
#         table_name = extract_table_name(excel_path)

#         # Connect to database
#         engine = get_engine(db_name)

#         # Define SQLAlchemy dtype mapping
#         sql_dtype = {}
#         for col in df.columns:
#             if col in int_cols:
#                 sql_dtype[col] = types.INTEGER()
#             else:
#                 sql_dtype[col] = types.NVARCHAR(length=500)

#         # Insert into SQL Server (replace table if exists)
#         df.to_sql(
#             table_name,
#             engine,
#             if_exists="replace",
#             index=False,
#             dtype=sql_dtype
#         )

#         print(f"✅ Inserted {len(df)} rows into table '{table_name}' in database '{db_name}'")
#         return engine, table_name

#     except Exception as e:
#         print(f"❌ SQL insertion failed for {excel_path}: {e}")
#         return None, None
    


# --------------------------------------------
# ============ Main Execution ================
# --------------------------------------------
if __name__ == "__main__":
    total_start_time = time.time()
    checkpoint = load_checkpoint()
    pdf_headers_dict = {}
    import paddle
    
    if paddle.is_compiled_with_cuda():
        print(f"✅ GPU available. Current device: {paddle.get_device()}")
        print(f"Number of GPUs detected: {paddle.device.cuda.device_count()}")
    else:
        print("⚠️ No GPU detected, using CPU")
    if paddle.is_compiled_with_cuda():
        print("🚀 PaddlePaddle is using GPU!")
    else:
        print("⚠️ PaddlePaddle is running on CPU")
        
    pdf_files = [os.path.join(pdf_folder, f) for f in os.listdir(pdf_folder) if f.lower().endswith(".pdf")]
    print(f"📂 Found {len(pdf_files)} PDF files")

    # Filter PDFs to process (skip already completed ones)
    pdf_files_to_process = []
    checkpoint_changed = False
    for pdf_file in pdf_files:
        pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]
        output_pdf_excel = os.path.join(os.path.dirname(output_excel), f"{pdf_name}.xlsx")

        if os.path.exists(output_pdf_excel):
            print(f"✔️ Skipping already processed PDF: {pdf_name}")
            if pdf_name in checkpoint:
                del checkpoint[pdf_name]
                checkpoint_changed = True
        else:
            pdf_files_to_process.append(pdf_file)

    # Update checkpoint file
    if checkpoint_changed:
        if checkpoint:
            with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                json.dump(checkpoint, f, indent=2)
        else:
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)

    print(f"📂 PDFs to process: {len(pdf_files_to_process)}")

    try:
        for pdf_file in pdf_files_to_process:
            start_time = time.time()
            pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]
            print(f"\n📄 Processing: {pdf_name}")

            temp_excel = os.path.join(os.path.dirname(output_excel), f"{pdf_name}_emergency.xlsx")
            pdf_voter_details = []

            # ---------------- Extract PDF Header ----------------
            pdf_header_info = {}
            header_extracted = False
            with fitz.open(pdf_file) as doc:
                for page_number in range(1, 20):
                    page = doc[page_number - 1]
                    pix_low = page.get_pixmap(matrix=fitz.Matrix(3.0, 3.0))
                    img_low = Image.frombytes("RGB", [pix_low.width, pix_low.height], pix_low.samples)

                    if card_is_present(img_low):
                        print(f"✅ Card found on page {page_number} of {pdf_name}. Extracting header...")
                        pix_full = page.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))
                        img_full = Image.frombytes("RGB", [pix_full.width, pix_full.height], pix_full.samples)
                        hdr = extract_header_info(img_full, top_margin=118.0, zoom_factor=zoom_factor)
                        pdf_header_info = {
                            "Municipal_Corporation": hdr.get("Municipal_Corporation", ""),
                            "Prabhag_No": hdr.get("Prabhag_No", ""),
                            "Prabhag_Name": hdr.get("Prabhag_Name", ""),
                            "File_Name": os.path.basename(pdf_file)
                        }
                        header_extracted = True
                        break

            if not header_extracted:
                print(f"⚠️ No cards found in {pdf_name}. Skipping header.")
            else:
                print(f"📑 Extracted PDF-level header for {pdf_name}: {pdf_header_info}")

            pdf_headers_dict[pdf_name] = pdf_header_info

            # ---------------- Process Pages ----------------
            with fitz.open(pdf_file) as doc:
                total_pages = len(doc)
                pages_to_iterate = list(range(1,11))  # all pages

                # Resume from checkpoint
                if pdf_name in checkpoint:
                    last_done = checkpoint[pdf_name]["last_page"]
                    print(f"🔄 Resuming {pdf_name} from page {last_done + 1}")
                    old_emergency = checkpoint[pdf_name]["temp_excel"]
                    if os.path.exists(old_emergency):
                        df_existing = pd.read_excel(old_emergency, dtype=str)
                        pdf_voter_details.extend(df_existing.to_dict("records"))
                    pages_to_iterate = [p for p in pages_to_iterate if p > last_done]

                for page_num in pages_to_iterate:
                    page_voters = process_page(pdf_file, page_num, zoom_factor, pdf_header_info)
                    if page_voters:
                        pdf_voter_details.extend(page_voters)
                        save_checkpoint(pdf_name, page_num, temp_excel)

                    # Emergency save + checkpoint
                    # if pdf_voter_details:
                    #     df_tmp = pd.DataFrame(pdf_voter_details)
                    #     if column_order:
                    #         ordered_cols = [col for col in column_order if col in df_tmp.columns]
                    #         other_cols = [col for col in df_tmp.columns if col not in ordered_cols]
                    #         df_tmp = df_tmp[ordered_cols + other_cols]

                    #     for col in df_tmp.columns:
                    #         df_tmp[col] = df_tmp[col].astype(str)

                    #     df_tmp.to_excel(temp_excel, index=False, engine="openpyxl")
                    #     save_checkpoint(pdf_name, page_num, temp_excel)
                    #     print(f"💾 Emergency save at page {page_num}: {temp_excel}")

            # ---------------- Final Save + SQL Insert ----------------
            if pdf_voter_details:
                df_pdf = pd.DataFrame(pdf_voter_details)
                if column_order:
                    ordered_cols = [col for col in column_order if col in df_pdf.columns]
                    other_cols = [col for col in df_pdf.columns if col not in ordered_cols]
                    df_pdf = df_pdf[ordered_cols + other_cols]

                for col in df_pdf.columns:
                    df_pdf[col] = df_pdf[col].astype(str)

                output_pdf_excel = os.path.join(os.path.dirname(output_excel), f"{pdf_name}.xlsx")
                df_pdf.to_excel(output_pdf_excel, index=False, engine="openpyxl")
                print(f"📄 Saved extracted data to: {output_pdf_excel}")

                # Insert into SQL: DB = Municipality, Table = Ward
                # try:
                #     insert_excel_to_sql(output_pdf_excel, exclude_cols=["Marathi_Text", "Paddle_Text","Cleaned_Text", "Raw_Header_Text"])
                #     print("📥 Data successfully inserted into SQL Server!")
                # except Exception as e:
                #     print(f"❌ SQL insertion failed: {e}")

                # Cleanup checkpoint + emergency
                checkpoint = load_checkpoint()
                if pdf_name in checkpoint:
                    temp_file = checkpoint[pdf_name].get("temp_excel")
                    if temp_file and os.path.exists(temp_file):
                        os.remove(temp_file)
                        print(f"🗑️ Deleted emergency file for completed PDF: {temp_file}")
                    del checkpoint[pdf_name]

                    if checkpoint:
                        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                            json.dump(checkpoint, f, indent=2)
                        print(f"✅ Updated checkpoint after finishing {pdf_name}")
                    else:
                        if os.path.exists(CHECKPOINT_FILE):
                            os.remove(CHECKPOINT_FILE)
                        print(f"🗑️ Deleted checkpoint file as all PDFs are processed")

            else:
                print(f"⚠️ No data extracted from {pdf_name}. Skipping file save.")

            # Timing
            elapsed_time = time.time() - start_time
            h, rem = divmod(elapsed_time, 3600)
            m, s = divmod(rem, 60)
            print(f"⏱️ Finished {pdf_name} in {int(h):02d}:{int(m):02d}:{int(s):02d}")

    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user! Saving emergency progress...")
        save_progress(pdf_voter_details, column_order, temp_excel)
        print("💾 Emergency file saved. You can resume later using checkpoint.")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        save_progress(pdf_voter_details, column_order, temp_excel)
        print("💾 Emergency file saved due to error.")

    # Total timing
    total_elapsed = time.time() - total_start_time
    th, rem = divmod(total_elapsed, 3600)
    tm, ts = divmod(rem, 60)
    print(f"\n🏁 All files processed in {int(th):02d}:{int(tm):02d}:{int(ts):02d}")