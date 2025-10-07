# === Marathi OCR Extraction Script ===
# Refactored with modular extractors + debug logging
# ===============================================
import fitz
from PIL import Image
import pytesseract
import pandas as pd
import numpy as np
from paddleocr import PaddleOCR
import re
import os, time, json
import cv2
import time
import pandas as pd
from sqlalchemy import create_engine,text,types
import logging
import pyodbc
from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.responses import FileResponse
import uvicorn
logging.getLogger("ppocr").setLevel(logging.WARNING)
import warnings
# --------------------------------------------
# =========== SQL Serve DB Config =============
# --------------------------------------------
DB_SERVER = "ORNET96"
DB_DRIVER = "ODBC Driver 17 for SQL Server"

DB_USER = "sa"                  # SQL Server username
DB_PASS = "manager"    # SQL Server password
DB_NAME = "Mh_139_2025"                # Default database (can be overridden)
TABLE_NAME = "Ward_Unknown" 

# Build connection string (ODBC)
connection_string = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_SERVER}/{DB_NAME}"
    f"?driver={DB_DRIVER.replace(' ', '+')}"
)

# Create SQLAlchemy engine with fast_executemany (better for bulk inserts)
engine = create_engine(connection_string, fast_executemany=True)

# -------------------------------------------
# =========== CONFIG(WORKSTATION2) ===========
# --------------------------------------------
# pdf_folder = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\pdf_extract"
# temp_excel = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\Output\output_temp1.xlsx"
# output_excel = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\Output\tmc_details.xlsx"
# card_image_folder = r"D:\PYTHON DEVELOPMENT\Coorperation_OCR_Extraction\Card_Images"
# os.makedirs(card_image_folder, exist_ok=True)


# -------------------------------------------
# =========== CONFIG(ORNET91) ===========
# --------------------------------------------
pdf_folder = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Mar_Coorperation_OCR\Input_Pdf"
temp_excel = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Mar_Coorperation_OCR\Output_Sample\output_temp1.xlsx"
output_excel = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Mar_Coorperation_OCR\Output_Sample\process_test1.xlsx"
card_image_folder = r"Extracted_Card_Img"
os.makedirs(card_image_folder, exist_ok=True)


# -------------------------------------------
# =========== PDF Render zoom ===========
# --------------------------------------------
zoom_factor = 3 


# -------------------------------------------
# =========== Tesseract OCR Setup ===========
# --------------------------------------------
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
tesseract_config = "--oem 1 --psm 3 -l mar"


# -------------------------------------------
# =========== Paddle OCR Setup ===========
# --------------------------------------------
ocr_paddle = PaddleOCR(use_angle_cls=True, lang='en', rec=True, gpu=True, precision='fp16', use_mp=True)


# ----------------------------------------
# ========= Prefix Mapping File =========
# ----------------------------------------
prefix_mapping_file = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Corporation_wise_prefix.xlsx"
sheet_name = "KBMC"
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
# ========= Precompiled regex for Voter Name patterns =========
# -------------------------------------------------------------
VOTER_NAME_PATTERN = re.compile(
        r"(?:मतदाराचे|उलदाराचे|टनदाराचे|ग्या|आ...|आ.|आ|मप हु|मट न्न|मटन|न ह|र छे|अर हे|यत्वाराचे|)\s*(?:पूर्ण|पुर्ण|पूण|उ|पूरण|पर्ण)\s*[：:;；]?\s*(.*?)(?=\s*(?:नजदाराचे|मतदाराचे|नांव|नाव|वडिलांचे\s*नाव|पतीचे\s*नाव|आईचे\s*नाव|घर\s*क्रमांक|वय|लिंग|$))",
        re.IGNORECASE
    )

LEADING_JUNK_PATTERN = re.compile(r"^[^अ-हक़-य़]+")
PUNCT_PATTERN        = re.compile(r"[॰॥]+")
MULTISPACE_PATTERN   = re.compile(r"\s+")
DIGIT_LATIN_PATTERN  = re.compile(r"^[0-9०-९a-zA-Z]+$")

# -------------------------------------------------------------
# ========= Precompiled regex for Relation Name patterns =========
# -------------------------------------------------------------
RELATION_KEYWORD_ALONE = re.compile(
    r"^(पतीचे नाव|पततीचे नाव|पत्तीचे नाव|वडिलांचे नाव|वडील|आईचे नाव|आईचे चाव|ईतर|इतर)$"
)

RELATION_PATTERNS = {
    "Husband": re.compile(r"(?:पतीचे नाव|पततीचे नाव|पत्तीचे नाव)\s*[:：;]?\s*(.+)"),
    "Father":  re.compile(r"(?:वडिलांचे नाव|वडील|बिलांचे नात)\s*[:：;]?\s*(.+)"),
    "Mother":  re.compile(r"(?:आईचे नाव)\s*[:：;]?\s*(.+)"),
    "Other":   re.compile(r"(?:ईतर|इतर)\s*[:：;]?\s*(.+)")
}

# -------------------------------------------------------------
# ========= Precompiled regex for House Number patterns =========
# -------------------------------------------------------------
HOUSE_NUMBER_PATTERN = re.compile(r"(?:घर\s*क्रमांक)[\s:'‘’“”\-]*([0-9०-९]+)", re.MULTILINE)
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


# ================================================================
# ============ HELPERS =================
# ================================================================


# ------------------------------------------------------------
# ============ Marathi to English Number Conversion ==========
# ------------------------------------------------------------
def marathi_to_english_number(text):
    marathi_digits = "०१२३४५६७८९"
    english_digits = "0123456789"
    return text.translate(str.maketrans(marathi_digits, english_digits))


# ------------------------------------------------------------
# ============ Preprocessing for upscaling ==========
# ------------------------------------------------------------
def preprocess_image(img: Image.Image,upscale_factor=3) -> Image.Image:
    new_size = (img.width * upscale_factor, img.height * upscale_factor)
    upscaled_img = img.resize(new_size, Image.Resampling.LANCZOS)
    return upscaled_img


# ------------------------------------------------------------
# ============ Find Voter Card Boxes in Page Image ==========
# ------------------------------------------------------------
# def find_card_boxes(pixmap_img, min_w=400, min_h=150, max_w=650, max_h=300, iou_thresh=0.3):
#     """
#     Detect voter card boxes from page image.
#     - Filters duplicate inner/outer contours
#     - Keeps only one bounding box per card
#     """
#     gray = cv2.cvtColor(pixmap_img, cv2.COLOR_RGB2GRAY)
#     blur = cv2.GaussianBlur(gray, (5, 5), 0)
#     edges = cv2.Canny(blur, 15, 150)

#     contours, _ = cv2.findContours(edges, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
#     raw_boxes = []
#     for cnt in contours:
#         x, y, w, h = cv2.boundingRect(cnt)
#         if min_w < w < max_w and min_h < h < max_h:
#             raw_boxes.append((x, y, x + w, y + h))

#     # Helper: IoU between two boxes
#     def iou(boxA, boxB):
#         xA = max(boxA[0], boxB[0])
#         yA = max(boxA[1], boxB[1])
#         xB = min(boxA[2], boxB[2])
#         yB = min(boxA[3], boxB[3])
#         interArea = max(0, xB - xA) * max(0, yB - yA)
#         areaA = (boxA[2] - boxA[0]) * (boxA[3] - boxA[1])
#         areaB = (boxB[2] - boxB[0]) * (boxB[3] - boxB[1])
#         unionArea = float(areaA + areaB - interArea)
#         return interArea / unionArea if unionArea > 0 else 0

#     # Deduplicate: keep only one box per overlapping region
#     deduped = []
#     for b in sorted(raw_boxes, key=lambda b: (b[1], b[0])):  # scan row-wise
#         if all(iou(b, d) < iou_thresh for d in deduped):
#             deduped.append(b)

#     # Sort again top-to-bottom, then left-to-right
#     final_boxes = sorted(deduped, key=lambda b: (b[1] // 250, b[0]))

#     return final_boxes

def find_card_boxes(pixmap_img):
    """
    Generate fixed grid boxes for voter cards using manual layout:
    - Card width and height defined manually
    - Separate upper/lower/side margins
    - Custom spacing between cards
    """
    page_height, page_width = pixmap_img.shape[:2]

    # Fixed PDF page dimensions in points
    pdf_width, pdf_height = 595.0, 842.0

    # Scale factors (points → pixels)
    scale_x = page_width / pdf_width
    scale_y = page_height / pdf_height

    # Manual layout settings (in PDF points)
    num_rows = 9
    num_cols = 3

    card_width = 184.6        # Manual card width
    card_height = 69         # Manual card height

    upper_margin = 82       # Top margin
    lower_margin = 50        # Bottom margin
    side_margin = 15         # Left and right margin

    horizontal_spacing = 2  # Space between columns
    vertical_spacing = 1.6    # Space between rows

    # Total content height check (for safety)
    total_height_needed = upper_margin + lower_margin + (num_rows * card_height) + ((num_rows - 1) * vertical_spacing)
    if total_height_needed > pdf_height:
        print(f"⚠️ Layout too tall for page: {total_height_needed} > {pdf_height}")

    coordinates = []
    for row in range(num_rows):
        for col in range(num_cols):
            x1 = side_margin + col * (card_width + horizontal_spacing)
            y1 = upper_margin + row * (card_height + vertical_spacing)
            x2 = x1 + card_width
            y2 = y1 + card_height

            # Scale to pixel space
            coordinates.append((
                int(x1 * scale_x),
                int(y1 * scale_y),
                int(x2 * scale_x),
                int(y2 * scale_y)
            ))

    return coordinates

# -----------------------------------------------------------
# =========== Check if Voter Card is Present in Image ========
# ------------------------------------------------------------
def card_is_present(image, min_cards=1):
    """
    Returns True if at least `min_cards` voter cards are detected in the image.
    """
    boxes = find_card_boxes(np.array(image))  # PIL to NumPy for OpenCV
    return len(boxes) >= min_cards


# =================================================
# ============== HEADER EXTRACTORS ================
# =================================================


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

# def extract_section_info(text):
#     section_no = ""
#     section_lines = []

#     lines = text.splitlines()
#     collecting = False

#     max_additional_lines = 10  # safety max lines after header
#     additional_lines_collected = 0
#     blank_line_count = 0

#     voter_data_pattern = re.compile(r"^\d{1,3}(,\d{1,4})?\s")
#     digit_pattern = re.compile(r"[0-9०-९]")  # ✅ matches English or Marathi digits

#     def normalize_section_name(name: str) -> str:
#         """Normalize OCR variants of 'NA' into 'NA'."""
#         if not name or not name.strip():
#             return ""

#         cleaned = name.strip().lower().replace(" ", "").replace(".", "")

#         # ✅ Add all known weird variants
#         na_variants = {
#             "na", "n/a", "एनए", "nil", "none", "---",
#             "1९%", "1५/", "1९/", "1९»", "1९%", "1९", "10»", "1»", "1९०"
#         }

#         if cleaned in na_variants:
#             return ""

#         return name.strip()

#     for idx, line in enumerate(lines):
#         line_stripped = line.strip()

#         if not collecting:
#             # Find "यादी भाग क्रमांक" line
#             if "यादी भाग क्रमांक" in line_stripped:
#                 collecting = True
#                 continue

#         elif collecting:
#             # Look for the section name line like "१- वालीवली गांव"
#             match = re.match(r"([०१२३४५६७८९0-9]+)\s*[-–—]\s*(.+)", line_stripped)
#             if match:
#                 marathi_section_no = match.group(1).strip()
#                 section_no = marathi_to_english_number(marathi_section_no)
#                 section_name = match.group(2).strip()
#                 section_name = normalize_section_name(section_name)
#                 return section_no, section_name

#             # If we don't get the expected pattern after N lines, stop
#             additional_lines_collected += 1
#             if additional_lines_collected >= max_additional_lines:
#                 break

#     # Fallback if no match found
#     return section_no or "", ""


# ------------------------------------------------------------------------
# =============== Booth Name and Address Extractors ==========================
# ------------------------------------------------------------------------
# def extract_booth_name(header_text):
#     booth_name = ""
#     booth_match = re.search(r"मतदान\s*केंद्र\s*[:\-]\s*([^\n]+)", header_text)
#     if booth_match:
#         booth_text = booth_match.group(1).strip()
#         if "पत्ता" in booth_text:
#             booth_name = booth_text.split("पत्ता")[0].strip()
#         else:
#             booth_name = booth_text
#     booth_name = re.sub(r"[A-Za-z0-9@#%^&*_=+\[\]{}<>;:.,/\\|-]", "", booth_name).strip()
#     debug_log(f"[BOOTH_NAME] {booth_name}")
#     return booth_name

# def extract_booth_address(header_text):
#     booth_address = ""
#     addr_match = re.search(r"पत्ता\s*[:\-]\s*([^\n]+)", header_text)
#     if addr_match:
#         booth_address = addr_match.group(1).strip()
#     booth_address = re.sub(r"[A-Za-z0-9@#%^&*_=+\[\]{}<>;:.,/\\|-]", "", booth_address).strip()
#     debug_log(f"[BOOTH_ADDR] {booth_address}")
#     return booth_address


# --------------------------------------------------------------
# =============== Extract Header Info from Page Image ============
# --------------------------------------------------------------
def extract_header_info(page_img, top_margin, zoom_factor):
    header_crop = page_img.crop((0, 0, page_img.width, int(top_margin * zoom_factor)))
    header_crop.save("debug_header_crop.png")  
    header_text = pytesseract.image_to_string(header_crop, config="--psm 6 -l mar").strip()
    # debug_log(f"[HEADER RAW]\n{header_text}")
    section_no, section_name = extract_section_info(header_text)
    # booth_name = extract_booth_name(header_text)
    # booth_address = extract_booth_address(header_text)
    municipal = extract_municipal(header_text)
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
        pages = list(range(1,20))
        pix = pages.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    hdr = extract_header_info(img, top_margin=118.0, zoom_factor=zoom_factor)
    pdf_header_info = {
        "Municipal_Corporation": hdr.get("Municipal_Corporation", ""),
        "Prabhag_No": hdr.get("Prabhag_No", ""),
        "Prabhag_Name": hdr.get("Prabhag_Name", ""),
        "File_Name": os.path.basename(pdf_file)
    }
    return pdf_header_info


# ================================================================
# ================== VOTER CARD EXTRACTORS =======================
# ===============================================================


# -----------------------------------------------------
# =========== Clean Marathi Name Extraction ============
# -----------------------------------------------------
def clean_name(name, max_words=None):
    """
    Clean Marathi or OCR-extracted name:
    - Remove quotes, dashes, colons, digits, slashes, dots
    - Remove stray single characters like 'ऱ्या', 'ल', etc.
    - Limit to max_words if specified
    """
    if not name:
        return ""
    
    # Remove leading/trailing quotes, dashes, colons, slashes, dots, digits
    name = re.sub(r"^[\"“”‘’(\-*:0-9\/,\.&॥]+", "", name)
    # name = re.sub(r"[\"“”‘’\-*:0-9\/,\.]+$", "", name)
    name = re.sub(r"^[^अ-ह]+", "", name)   # leading junk
    # name = re.sub(r"[^अ-ह\s]+$", "", name) # trailing junk
    name = re.sub(r"^[^अ-हक़-य़]+", "", name)  # leading junk (extended)

    
    # Remove stray single-character artifacts
    words = name.split()
    words = [w for w in words if len(w) > 1]  # keep words with length > 1

    # Limit to max_words
    words = words[:max_words]
    
    return " ".join(words).strip()


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
# =========== Correct Name using Dictionary ============
# -----------------------------------------------------
with open("D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Mar_Coorperation_OCR\corrections.json", "r", encoding="utf-8") as f:
    correction_dict = json.load(f)

def correct_name_with_dict(name: str) -> str:
    """Correct common OCR mistakes using dictionary from JSON."""
    return " ".join([correction_dict.get(word, word) for word in name.split()])


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


# ------------------------------------------------------------------
# =========== Extract Voter Name from Tesseract OCR Text ============
# ------------------------------------------------------------------
# def extract_voter_name(text):
#     """Extract full voter name from Marathi OCR text (optimized)."""
#     # Clean basic unwanted chars
#     text = text.replace("\n", " ").replace("[", "").replace("]", "").replace("'", "")
#     text = MULTISPACE_PATTERN.sub(" ", text).strip()

#     # Match name
#     match = VOTER_NAME_PATTERN.search(text)
#     if not match:
#         return ""

#     main_text = match.group(1).strip()
#     main_text = LEADING_JUNK_PATTERN.sub("", main_text)     # remove junk at start
#     main_text = PUNCT_PATTERN.sub("", main_text).strip()    # remove Marathi punctuation
#     main_text = MULTISPACE_PATTERN.sub(" ", main_text)      # collapse spaces

#     # Drop numbers/Latin-only tokens
#     words = [w for w in main_text.split() if not DIGIT_LATIN_PATTERN.match(w)]
#     voter_name = " ".join(words[:4])  # keep first 3–4 words

#     # Apply dictionary correction
#     return correct_name_with_dict(voter_name)


def extract_voter_name(text):
    """Extract full voter name from Marathi OCR text (with digit stop logic)."""
    # Clean unwanted chars
    text = text.replace("\n", " ").replace("[", "").replace("]", "").replace("'", "")
    text = MULTISPACE_PATTERN.sub(" ", text).strip()

    # Match name pattern
    match = VOTER_NAME_PATTERN.search(text)
    if not match:
        return ""

    main_text = match.group(1).strip()
    main_text = LEADING_JUNK_PATTERN.sub("", main_text)     # remove junk at start
    main_text = PUNCT_PATTERN.sub("", main_text).strip()    # remove Marathi punctuation
    main_text = MULTISPACE_PATTERN.sub(" ", main_text)      # collapse spaces

    # Split into words and stop at first word with digit
    name_parts = []
    for word in main_text.split():
        if re.search(r"[0-9०-९]", word):  # Stop at any English or Marathi digit
            break
        if not DIGIT_LATIN_PATTERN.match(word):  # Avoid Latin-only junk
            name_parts.append(word)

    voter_name = " ".join(name_parts[:4])  # Max 4 words

    # Apply correction (if you have a dictionary correction method)
    return correct_name_with_dict(voter_name)

# ---------------------------------------------------------------
# =========== Split Marathi Full Name into First/Last ===========
# ----------------------------------------------------------------

def split_voter_name(full_name: str):
    """
    Split Marathi full name into First, Last, Middle.
    Convention: <Last> <First> <Middle/Father's Name>
    Example: 'पाटील सुरेश महादेव' -> First='सुरेश', Last='पाटील', Middle='महादेव'
    """
    words = full_name.strip().split()

    if not words:
        return "", "", ""

    if len(words) == 1:
        # Only one word: assume it's the first name
        return words[0], "", ""

    if len(words) == 2:
        # Two words: assume <Last> <First>
        last, first = words
        return first, last, ""

    # Three or more words: assume <Last> <First> <Middle...>
    last, first, *middle = words
    middle = " ".join(middle)

    return first, last, middle


def split_relation_name(full_name: str, voter_last: str, voter_middle: str):
    """
    Split relation name with help of voter details.
    Rules:
      - If voter_last exists in relation name → make it relation last name
      - Else fallback to <Last> <First>
      - If voter has no middle → borrow relation first name as voter's middle
    """
    words = full_name.strip().split()
    first, last = "", ""

    if not words:
        return "", "", voter_middle  # keep voter middle unchanged if relation empty

    # Case 1: voter last name is present in relation words
    if voter_last and voter_last in words:
        last = voter_last
        remaining = [w for w in words if w != voter_last]
        first = remaining[0] if remaining else ""
    else:
        # Fallback <Last> <First>
        if len(words) == 1:
            first = words[0]
        else:
            last, first = words[0], words[1]

    # Borrow relation first if voter middle empty
    if not voter_middle and first:
        voter_middle = first

    return first, last, voter_middle

# --------------------------------------------------------
# =========== Extract Relation Type and Name ============
# --------------------------------------------------------
def extract_relation_info(text):
    """Extract relation type and full relation name from Marathi OCR text (optimized)."""

    # --- Normalize lines ---
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    merged_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # Case: keyword alone (next line is value)
        if RELATION_KEYWORD_ALONE.match(line):
            if i + 1 < len(lines):
                next_val = lines[i + 1].strip()
                merged_lines.append(f"{line} {next_val}")
                i += 2
                continue

        merged_lines.append(line)
        i += 1

    clean_text = "\n".join(merged_lines)

    # --- Regex patterns for relation types ---
    for r_type, pattern in RELATION_PATTERNS.items():
        match = pattern.search(clean_text)
        if match:
            relation_name = match.group(1).lstrip()   # remove leading spaces
            relation_name = clean_name(relation_name, max_words=2)
            relation_name = correct_name_with_dict(relation_name)
            return r_type, relation_name

    return "", ""


# --------------------------------------------------------
# =========== Limit Relation Name to 2 Words ============
# --------------------------------------------------------
def limit_to_two_words(name):
    """Limit extracted relation name to max 2 words."""
    return " ".join(name.split()[:2]) if name else ""


# --------------------------------------------------------------------
# =========== Extract House Number from Tesseract OCR Text ============
# --------------------------------------------------------------------
def extract_house_number(text):
    """Extract house number (Devanagari or English digits)."""
    match = HOUSE_NUMBER_PATTERN.search(text)
    return marathi_to_english_number(match.group(1).strip()) if match else ""


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
def parse_voter_card(marathi_text, cleaned_text):
    voter_name = extract_voter_name(marathi_text)
    voter_first, voter_last, voter_middle = split_voter_name(voter_name)
    relation_type, relation_name = extract_relation_info(cleaned_text)
    rel_first, rel_last, voter_middle = split_relation_name(
    relation_name, voter_last, voter_middle)
    house_number = extract_house_number(cleaned_text)
    age_marathi, age_english = extract_age(cleaned_text)
    raw_gender, normalized_gender = extract_gender(cleaned_text)
    gender_english = marathi_to_english_gender(normalized_gender)

    return {
        "FullnameMar": voter_name,
        "NameM": voter_first,
        "MiddleNameM": voter_middle,
        "SurnameM": voter_last,
        "rln_type": relation_type,
        "RLN_FM_NM_v1": relation_name,
        "RLN_F_NM_V1": rel_first,
        "RLN_L_NM_V1": rel_last,
        "House_Number": house_number,
        "AgeM": age_marathi,
        "Age": age_english,
        "GenderM": normalized_gender,  # normalized Marathi root word
        "Gender": gender_english,     # English category
    }


# ================================================================
# === EPIC & SERIAL EXTRACTORS ===
# ================================================================

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
    "Voter_ID", "Section_No", "Section_Name","List_No","Page",
    "Ac_no","EPIC_Number",
    "FullnameMar", "NameM", "MiddleNameM", "SurnameM",
    "rln_type", "RLN_FM_NM_v1", "RLN_F_NM_V1", "RLN_L_NM_V1",
    "House_Number",
    "AgeM", "Age",
    "GenderM", "Gender",
    # "Booth_Name", "Booth_Address",
     "Card_Index",
    "Marathi_Text","Cleaned_Text", "Paddle_Text",
]


# ---------------------------------------------------------------
# ================ Main Page Processing Function ===============
# ---------------------------------------------------------------

def process_page(pdf_file, page_num, zoom_factor, pdf_header_info=None, save_cards_dir=None):
    """
    Process a single PDF page:
    - Rasterize page with zoom factor
    - Detect voter card bounding boxes using find_card_boxes
    - OCR each detected card and extract relevant info
    - Attach page-level section info extracted from the top margin
    - Save cropped card images if save_cards_dir is provided
    """
    voter_details = []
    serial_counter = 1

    import fitz  # PyMuPDF
    doc = fitz.open(pdf_file)
    page = doc[page_num - 1]

    # Render page pixmap at desired zoom
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))

    # Convert pixmap to numpy array (H x W x C)
    pix_np = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if pix_np.shape[2] == 4:
        pix_np = pix_np[:, :, :3]

    print(f"[process_page] Page {page_num} image size: width={pix.width}, height={pix.height}")

    if pix.width is None or pix.height is None:
        print(f"⚠️ Invalid page dimensions for page {page_num}, skipping...")
        doc.close()
        return []

    # Convert full pixmap to PIL Image for cropping
    full_img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    # 🔍 Extract Section Info from Top Header Area of the Page
    header_crop_height = int(118 * zoom_factor)
    header_crop = full_img.crop((0, 0, full_img.width, header_crop_height))
    header_text = pytesseract.image_to_string(header_crop, config="--psm 6 -l mar").strip()
    municipal = extract_municipal(header_text)
    prabhag_no, prabhag_name = extract_prabhag_info(header_text)
    section_no, section_name = extract_section_info(header_text)


    # # (Optional) Save debug header image
    # debug_filename = f"debug_header_page{page_num}.png"
    # header_crop.save(debug_filename)
    # print(f"🖼️ Saved debug header crop: {debug_filename}")
    # print(f"[Header Page {page_num}] Section_No: {section_no}, Section_Name: {section_name}")

    try:
        card_coords_points = find_card_boxes(pix_np)
    except Exception as e:
        print(f"⚠️ Error during card box detection on page {page_num}: {e}")
        doc.close()
        return []

    if not card_coords_points:
        print(f"⚠️ No card boxes detected on page {page_num}")
        doc.close()
        return []

    if save_cards_dir:
        os.makedirs(save_cards_dir, exist_ok=True)

    for card_index, (x1, y1, x2, y2) in enumerate(card_coords_points, start=1):
        card_img = full_img.crop((x1, y1, x2, y2))

        # if save_cards_dir:
        #     card_filename = os.path.join(
        #         save_cards_dir,
        #         f"{os.path.splitext(os.path.basename(pdf_file))[0]}_page{page_num}_card{card_index}.png"
        #     )
        #     card_img.save(card_filename)
        #     print(f"🖼️ Saved card image: {card_filename}")

        # Preprocess + OCR
        preprocessed_img = preprocess_image(card_img)
        marathi_text = pytesseract.image_to_string(preprocessed_img, config=tesseract_config).strip()
        if not marathi_text:
            continue

        cleaned_text = clean_tesseract_text(marathi_text)
        result_paddle = ocr_paddle.ocr(np.array(preprocessed_img))
        paddle_text = "\n".join([line[1][0] for line in result_paddle[0]]) if result_paddle and result_paddle[0] else ""

        voter_name = extract_voter_name(cleaned_text)
        parsed = parse_voter_card(marathi_text, cleaned_text)

        epic_number = extract_epic_number(paddle_text)
        list_number = extract_list_number(paddle_text)
        ac_no = extract_assembly_consitution_no(paddle_text)
        index_number = extract_index_number(paddle_text)

        serial_number, _, serial_counter = extract_serial_number(
            paddle_text, paddle_text, serial_counter, False
        )

        parsed.update({
            "New_Voter_ID": index_number,
            "EPIC_Number": correct_epic_number(epic_number) if epic_number else None,
            "Voter_ID": serial_number,
            "Page": page_num,
            "Card_Index": card_index,
            "Marathi_Text": marathi_text,
            "Cleaned_Text": cleaned_text,
            "FullnameMar": voter_name,
            "Paddle_Text": paddle_text,
            "List_No": list_number,
            "Ac_no": ac_no,
            "Section_No": section_no,
            "Section_Name": section_name,
            "Raw_Header_Text": header_text,
            "Municipal_Corporation": municipal,
            "Prabhag_No": prabhag_no,
            "Prabhag_Name": prabhag_name,
            "File_Name": os.path.basename(pdf_file),
        })

        voter_details.append(parsed)

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
def get_engine(db_name, user=DB_USER, password=DB_PASS):
    """
    Create SQLAlchemy engine using SQL Server Authentication.
    """
    conn_str = f"mssql+pyodbc://{user}:{password}@{DB_SERVER}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(conn_str, fast_executemany=True)

# === Ensure Database Exists ===
def ensure_database_exists(db_name, user=DB_USER, password=DB_PASS):
    """
    Creates the database if it doesn't exist using raw pyodbc (autocommit=True).
    Avoids 'CREATE DATABASE inside transaction' error.
    """
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};UID={user};PWD={password};DATABASE=master"
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name='{db_name}')
                CREATE DATABASE [{db_name}]
        """)
        print(f"✅ Database ready: {db_name}")

# === Clean + Validate Integer Columns ===
def enforce_integer_columns(df, int_cols):
    for col in int_cols:
        if col in df.columns:
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            bad_mask = numeric_series.isna() & df[col].notna()
            if bad_mask.any():
                bad_vals = df[col][bad_mask].unique()
                raise ValueError(f"❌ Column '{col}' contains non-integer values: {bad_vals}")
            if not (numeric_series.dropna() == numeric_series.dropna().astype(int)).all():
                bad_vals = df[col][numeric_series != numeric_series.astype(int)].unique()
                raise ValueError(f"❌ Column '{col}' contains non-integer decimal values: {bad_vals}")
            df[col] = numeric_series.astype("Int64")
    return df

# === Extract Table Name from Excel/PDF File Name ===
def extract_table_name(excel_path):
    """
    Example: DraftList_Ward_28_KDMC.xlsx -> Ward_28
    """
    base = os.path.splitext(os.path.basename(excel_path))[0]
    ward_match = re.search(r"Ward[_ ]?(\d+)", base, re.IGNORECASE)
    return f"Ward_{ward_match.group(1)}" if ward_match else "Ward_Unknown"

# === Insert Excel into SQL Server ===
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

def insert_excel_to_sql(excel_path, db_name=DB_NAME, exclude_cols=None, table_name=None):
    """
    Reads an Excel file and inserts it into SQL Server.
    All text columns (Marathi included) are stored as NVARCHAR.
    Integer columns remain INT.
    Replaces the table if it already exists.
    Returns (engine, table_name) for further processing.
    
    Parameters:
    - excel_path: path to Excel file
    - db_name: target database name
    - exclude_cols: list of columns to drop before insert
    - table_name: name of the SQL table to insert into (overrides default)
    """
    try:
        print(f"📂 Reading Excel file: {excel_path}")
        df = pd.read_excel(excel_path, dtype=str)

        if df.empty:
            print("⚠️ Excel file is empty, nothing to insert.")
            return None, None

        if exclude_cols:
            df = df.drop(columns=exclude_cols, errors="ignore")

        # Columns that must be integers
        int_cols = [
            "New_Voter_ID", "Voter_ID", "Section_No", "List_No",
            "Page", "Card_Index", "Prabhag_No", "Ac_no", "Age"
        ]
        df = enforce_integer_columns(df, int_cols)

        # Ensure database exists
        ensure_database_exists(db_name)

        # Use provided table_name or extract from filename
        if not table_name:
            table_name = extract_table_name(excel_path)

        # Connect to database
        engine = get_engine(db_name)

        # Define SQLAlchemy dtype mapping
        sql_dtype = {}
        for col in df.columns:
            if col in int_cols:
                sql_dtype[col] = types.INTEGER()
            else:
                sql_dtype[col] = types.NVARCHAR(length=500)

        # Insert into SQL Server (replace table if exists)
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
            dtype=sql_dtype
        )

        print(f"✅ Inserted {len(df)} rows into table '{table_name}' in database '{db_name}'")
        return engine, table_name

    except Exception as e:
        print(f"❌ SQL insertion failed for {excel_path}: {e}")
        return None, None

# === Add Flags ===
def add_flags(engine, table_name):
    """Add a Flag column and update values based on rules."""
    with engine.begin() as conn:
        # Add Flag column if not exists
        conn.execute(text(f"""
            IF COL_LENGTH('{table_name}', 'Flag') IS NULL
                ALTER TABLE {table_name} ADD Flag VARCHAR(255);
        """))

        # Ensure Missing_Successors column exists
        conn.execute(text(f"""
            IF COL_LENGTH('{table_name}', 'Missing_Successors') IS NULL
            BEGIN
                ALTER TABLE {table_name}
                ADD Missing_Successors INT NULL;
            END
        """))

        # Update Missing_Successors
        conn.execute(text(f"""
        ;WITH Sorted AS (
            SELECT New_Voter_ID,
                   LEAD(New_Voter_ID) OVER (ORDER BY New_Voter_ID ASC) AS next_id
            FROM {table_name}
        )
        UPDATE t
        SET Missing_Successors = CASE 
                                    WHEN s.next_id - t.New_Voter_ID > 1 
                                    THEN s.next_id - t.New_Voter_ID - 1
                                    ELSE 0
                                  END
        FROM {table_name} t
        JOIN Sorted s
          ON t.New_Voter_ID = s.New_Voter_ID;
        """))

        # Update Flag column
        conn.execute(text(f"""
        UPDATE {table_name}
        SET Flag = NULLIF(
            CONCAT_WS(',',
                CASE WHEN FullnameMar IS NULL OR FullnameMar = '' THEN 'MISSING_VN' END,
                CASE WHEN RLN_FM_NM_v1 IS NULL OR RLN_FM_NM_v1 = '' THEN 'MISSING_RN' END,
                CASE WHEN EPIC_Number IS NULL OR EPIC_Number = '' THEN 'MISSING_EPIC' END,
                CASE WHEN LEN(LTRIM(RTRIM(FullnameMar))) < 4 THEN 'VN_SHORT' END,
                CASE WHEN LEN(LTRIM(RTRIM(RLN_FM_NM_v1))) < 4 THEN 'RN_SHORT' END,
                CASE WHEN SurnameM <> RLN_L_NM_V1 THEN 'VLN-RLN_MISMATCH' END
            ),
            ''
        );
        """))

        print(f"✅ Flags updated in table '{table_name}'")

# --------------------------------------------
# --------------Main Loop (Merge)----------------
# --------------------------------------------
if __name__ == "__main__":
    total_start_time = time.time()
    checkpoint = load_checkpoint()
    pdf_headers_dict = {}
    all_voter_details = []

    # # Input/output folders
    # pdf_folder = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Input_Pdf"
    # output_excel = r"D:\Sahil_Tejam\ALL_OCR\Marathi_OCR\Output_Sample\combined_output.xlsx"

    # Get all PDFs
    pdf_files = [os.path.join(pdf_folder, f) for f in os.listdir(pdf_folder) if f.lower().endswith(".pdf")]
    print(f"📂 Found {len(pdf_files)} PDF files")

    # Filter already processed PDFs
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

    # Save checkpoint if changed
    if checkpoint_changed:
        if checkpoint:
            with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                json.dump(checkpoint, f, indent=2)
        elif os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)

    print(f"📂 PDFs to process: {len(pdf_files_to_process)}")

    try:
        for pdf_file in pdf_files_to_process:  # ✅ Process one PDF at a time
            start_time = time.time()
            pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]
            print(f"\n📄 Processing: {pdf_name}")

            temp_excel = os.path.join(os.path.dirname(output_excel), f"{pdf_name}_emergency.xlsx")
            pdf_voter_details = []

            # ---------------- Process Pages ----------------
            with fitz.open(pdf_file) as doc:
                total_pages = len(doc)
                pages_to_iterate = list(range(1, 15))

                if pdf_name in checkpoint:
                    last_done = checkpoint[pdf_name]["last_page"]
                    print(f"🔄 Resuming {pdf_name} from page {last_done + 1}")
                    old_emergency = checkpoint[pdf_name]["temp_excel"]
                    if os.path.exists(old_emergency):
                        df_existing = pd.read_excel(old_emergency, dtype=str)
                        df_existing = df_existing.fillna('')
                        pdf_voter_details.extend(df_existing.to_dict("records"))
                    pages_to_iterate = [p for p in pages_to_iterate if p > last_done]

                for page_num in pages_to_iterate:
                    page = doc[page_num - 1]
                    pix = page.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

                    header_crop = img.crop((0, 0, img.width, int(118 * zoom_factor)))
                    header_text = pytesseract.image_to_string(header_crop, config="--psm 6 -l mar").strip()

                    section_no, section_name = extract_section_info(header_text)
                    municipal = extract_municipal(header_text)
                    prabhag_no, prabhag_name = extract_prabhag_info(header_text)

                    if not municipal or not section_no or not section_name:
                        print(f"⚠️ Skipping page {page_num} — no header info found.")
                        continue

                    print(f"[Page {page_num}] Section No: {section_no}, Section Name: {section_name}")

                    pdf_header_info = {
                        "Section_No": section_no,
                        "Section_Name": section_name,
                        "Municipal_Corporation": municipal,
                        "Prabhag_No": prabhag_no,
                        "Prabhag_Name": prabhag_name,
                        "File_Name": os.path.basename(pdf_file),
                        "Raw_Header_Text": header_text
                    }

                    save_cards_dir = os.path.join("saved_cards", pdf_name)
                    page_voters = process_page(
                        pdf_file,
                        page_num,
                        zoom_factor,
                        pdf_header_info=pdf_header_info,
                        save_cards_dir=save_cards_dir
                    )

                    if page_voters:
                        pdf_voter_details.extend(page_voters)
                        save_checkpoint(pdf_name, page_num, temp_excel)

            # ---------------- Save Excel + Insert to SQL ----------------
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

                # Cleanup temporary checkpoint file
                temp_file = checkpoint.get(pdf_name, {}).get("temp_excel")
                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)

                # Remove this PDF from checkpoint
                if pdf_name in checkpoint:
                    del checkpoint[pdf_name]

                if checkpoint:
                    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                        json.dump(checkpoint, f, indent=2)
                elif os.path.exists(CHECKPOINT_FILE):
                    os.remove(CHECKPOINT_FILE)

            else:
                print(f"⚠️ No data extracted from {pdf_name}.")

            # Timing
            elapsed_time = time.time() - start_time
            h, rem = divmod(elapsed_time, 3600)
            m, s = divmod(rem, 60)
            print(f"⏱️ Finished {pdf_name} in {int(h):02d}:{int(m):02d}:{int(s):02d}")

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user. Saving emergency progress...")
        save_progress(pdf_voter_details, column_order, temp_excel)
        print("💾 Emergency file saved.")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        save_progress(pdf_voter_details, column_order, temp_excel)
        print("💾 Emergency file saved due to error.")

    # ---------------- Merge All Excel Files ----------------
    try:
        print("\n📦 Merging all generated Excel files...")

        output_folder = os.path.dirname(output_excel)
        merged_df = pd.DataFrame()

        for file in os.listdir(output_folder):
            if file.endswith(".xlsx") and not file.endswith("_emergency.xlsx") and "Merged_Output" not in file:
                file_path = os.path.join(output_folder, file)
                df = pd.read_excel(file_path, dtype=str)
                merged_df = pd.concat([merged_df, df], ignore_index=True)

                if not merged_df.empty:
                    merged_excel_path = os.path.join(output_folder, "All_Merged_Output.xlsx")
                    merged_df.to_excel(merged_excel_path, index=False, engine="openpyxl")
                    print(f"✅ Merged Excel saved at: {merged_excel_path}")

                    # 🧹 Delete all individual per-PDF Excel files
                    for file in os.listdir(output_folder):
                        if file.endswith(".xlsx") and file != "All_Merged_Output.xlsx":
                            try:
                                os.remove(os.path.join(output_folder, file))
                            except Exception as del_err:
                                print(f"⚠️ Could not delete {file}: {del_err}")

                    print("🧹 Cleaned up individual Excel files. Only merged file kept.")

                    # ---------------- Insert merged file into SQL ----------------
                    try:
                        engine, table_name = insert_excel_to_sql(
                            merged_excel_path,
                            exclude_cols=["Marathi_Text", "Paddle_Text", "Cleaned_Text", "Raw_Header_Text"]
                        )
                        if engine is not None and table_name is not None:
                            print(f"📥 Inserted merged data into SQL table: {table_name}")
                            try:
                                add_flags(engine, table_name)
                                print(f"✅ Flags updated in merged table: {table_name}")
                            except Exception as flag_e:
                                print(f"❌ Flag update failed for merged table: {flag_e}")
                    except Exception as sql_e:
                        print(f"❌ SQL insertion for merged file failed: {sql_e}")

                else:
                    print("⚠️ No Excel files found to merge.")

    except Exception as e:
        print(f"❌ Merging step failed: {e}")

    # Total timing
    total_elapsed = time.time() - total_start_time
    th, rem = divmod(total_elapsed, 3600)
    tm, ts = divmod(rem, 60)
    print(f"\n🏁 All files processed in {int(th):02d}:{int(tm):02d}:{int(ts):02d}")








# ---------------------------------------------------
# --------------Main Loop (Seperate Excel) ----------------
# ---------------------------------------------------
# if __name__ == "__main__":
#     total_start_time = time.time()
#     checkpoint = load_checkpoint()
#     pdf_headers_dict = {}
#     all_voter_details = []

#     # Get all PDFs
#     pdf_files = [os.path.join(pdf_folder, f) for f in os.listdir(pdf_folder) if f.lower().endswith(".pdf")]
#     print(f"📂 Found {len(pdf_files)} PDF files")

#     # Filter already processed PDFs
#     pdf_files_to_process = []
#     checkpoint_changed = False
#     for pdf_file in pdf_files:
#         pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]
#         output_pdf_excel = os.path.join(os.path.dirname(output_excel), f"{pdf_name}.xlsx")

#         if os.path.exists(output_pdf_excel):
#             print(f"✔️ Skipping already processed PDF: {pdf_name}")
#             if pdf_name in checkpoint:
#                 del checkpoint[pdf_name]
#                 checkpoint_changed = True
#         else:
#             pdf_files_to_process.append(pdf_file)

#     # Save checkpoint if changed
#     if checkpoint_changed:
#         if checkpoint:
#             with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
#                 json.dump(checkpoint, f, indent=2)
#         elif os.path.exists(CHECKPOINT_FILE):
#             os.remove(CHECKPOINT_FILE)

#     print(f"📂 PDFs to process: {len(pdf_files_to_process)}")

#     try:
#         for pdf_file in pdf_files_to_process:  # ✅ Only unprocessed PDFs
#             start_time = time.time()
#             pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]
#             print(f"\n📄 Processing: {pdf_name}")

#             temp_excel = os.path.join(os.path.dirname(output_excel), f"{pdf_name}_emergency.xlsx")
#             pdf_voter_details = []

#            # ---------------- Process Pages with Per-Page Section Info ----------------
#             with fitz.open(pdf_file) as doc:
#                 total_pages = len(doc)
#                 pages_to_iterate = list(range(1, 14))  # all pages

#                 if pdf_name in checkpoint:
#                     last_done = checkpoint[pdf_name]["last_page"]
#                     print(f"🔄 Resuming {pdf_name} from page {last_done + 1}")
#                     old_emergency = checkpoint[pdf_name]["temp_excel"]
#                     if os.path.exists(old_emergency):
#                         df_existing = pd.read_excel(old_emergency, dtype=str)
#                         df_existing = df_existing.fillna('')
#                         pdf_voter_details.extend(df_existing.to_dict("records"))
#                     pages_to_iterate = [p for p in pages_to_iterate if p > last_done]

#                 for page_num in pages_to_iterate:
#                     page = doc[page_num - 1]
#                     pix = page.get_pixmap(matrix=fitz.Matrix(zoom_factor, zoom_factor))
#                     img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

#                     header_crop = img.crop((0, 0, img.width, int(118 * zoom_factor)))
#                     header_text = pytesseract.image_to_string(header_crop, config="--psm 6 -l mar").strip()
#                     section_no, section_name = extract_section_info(header_text)
#                     municipal = extract_municipal(header_text)
#                     prabhag_no, prabhag_name = extract_prabhag_info(header_text)    

#                     if not municipal or not section_no or not section_name:
#                         print(f"⚠️ Skipping page {page_num} — no header info found.")
#                         continue

#                     print(f"[Page {page_num}] Section No: {section_no}, Section Name: {section_name}")

#                     pdf_header_info = {
#                    "Section_No": section_no,
#                    "Section_Name": section_name,
#             "Municipal_Corporation": municipal,
#             "Prabhag_No": prabhag_no,
#             "Prabhag_Name": prabhag_name,
#                    "File_Name": os.path.basename(pdf_file),
#                    "Raw_Header_Text": header_text
#                }

#                     save_cards_dir = os.path.join("saved_cards", pdf_name)
#                     page_voters = process_page(
#     pdf_file,
#     page_num,
#     zoom_factor,
#     pdf_header_info=pdf_header_info,
#     save_cards_dir=save_cards_dir
# )

#                     if page_voters:
#                         pdf_voter_details.extend(page_voters)
#                         save_checkpoint(pdf_name, page_num, temp_excel)


#             # ---------------- Save Excel + Insert to SQL ----------------
#             if pdf_voter_details:
#                 df_pdf = pd.DataFrame(pdf_voter_details)
#                 if column_order:
#                     ordered_cols = [col for col in column_order if col in df_pdf.columns]
#                     other_cols = [col for col in df_pdf.columns if col not in ordered_cols]
#                     df_pdf = df_pdf[ordered_cols + other_cols]

#                 for col in df_pdf.columns:
#                     df_pdf[col] = df_pdf[col].astype(str)

#                 output_pdf_excel = os.path.join(os.path.dirname(output_excel), f"{pdf_name}.xlsx")
#                 df_pdf.to_excel(output_pdf_excel, index=False, engine="openpyxl")
#                 print(f"📄 Saved extracted data to: {output_pdf_excel}")

#                 # SQL Insertion
#                 try:
#                     engine, table_name = insert_excel_to_sql(
#                         output_pdf_excel,
#                         exclude_cols=["Marathi_Text", "Paddle_Text", "Cleaned_Text", "Raw_Header_Text"]
#                     )
#                     if engine is not None and table_name is not None:
#                         print(f"📥 Inserted into SQL Server table: {table_name}")
#                         try:
#                             add_flags(engine, table_name)
#                             print(f"✅ Flags updated in table: {table_name}")
#                         except Exception as flag_e:
#                             print(f"❌ Flag update failed: {flag_e}")
#                 except Exception as e:
#                     print(f"❌ SQL insertion failed: {e}")

#                 # Cleanup checkpoint and emergency file
#                 temp_file = checkpoint.get(pdf_name, {}).get("temp_excel")
#                 if temp_file and os.path.exists(temp_file):
#                     os.remove(temp_file)
#                     print(f"🗑️ Deleted emergency file: {temp_file}")

#                 # Remove this PDF from checkpoint
#                 if pdf_name in checkpoint:
#                     del checkpoint[pdf_name]

#                 # Save or delete checkpoint file
#                 if checkpoint:
#                     with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
#                         json.dump(checkpoint, f, indent=2)
#                 else:
#                     if os.path.exists(CHECKPOINT_FILE):
#                         os.remove(CHECKPOINT_FILE)
#                     print(f"🗑️ Deleted checkpoint file (all done)")

#             else:
#                 print(f"⚠️ No data extracted from {pdf_name}.")

#             # Timing
#             elapsed_time = time.time() - start_time
#             h, rem = divmod(elapsed_time, 3600)
#             m, s = divmod(rem, 60)
#             print(f"⏱️ Finished {pdf_name} in {int(h):02d}:{int(m):02d}:{int(s):02d}")

#     except KeyboardInterrupt:
#         print("\n⚠️ Interrupted by user. Saving emergency progress...")
#         save_progress(pdf_voter_details, column_order, temp_excel)
#         print("💾 Emergency file saved.")

#     except Exception as e:
#         print(f"\n❌ Unexpected error: {e}")
#         save_progress(pdf_voter_details, column_order, temp_excel)
#         print("💾 Emergency file saved due to error.")

#     # Total timing
#     total_elapsed = time.time() - total_start_time
#     th, rem = divmod(total_elapsed, 3600)
#     tm, ts = divmod(rem, 60)
#     print(f"\n🏁 All files processed in {int(th):02d}:{int(tm):02d}:{int(ts):02d}")
