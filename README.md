

# Uniscraper

Automation script to scrape university related information

## 🚀 Getting Started

Follow these instructions to get the project up and running on your local machine.

### Prerequisites

* **Python 3.14+**
* **pip** (Python package manager)

### Installation & Setup

1. **Clone the repository** (or navigate to the project folder):
```bash
cd Uniscraper

```


2. **Create a Virtual Environment**
It is recommended to use a virtual environment to keep dependencies isolated.
```bash
python3 -m venv venv

```


3. **Activate the Virtual Environment**
* **macOS/Linux:**
```bash
source venv/bin/activate

```


* **Windows:**
```bash
.\venv\Scripts\activate

```




4. **Install Requirements**
Install the necessary libraries using the `requirements.txt` file:
```bash
pip install -r requirements.txt

```

5. **Create a .env file
Replace the api key with your own key
```bash
GOOGLE_API_KEY="<Gemini_Api_key>"
MODEL="gemini-2.5-pro"

```


---

## 🛠 Usage

To run the script, provide the **University Name** as a command-line argument.

```bash
python3 Uniscraper.py "University Name"

```


### Example

```bash
python3 Uniscraper.py "Harvard University"

```

---

## 📂 Project Structure

### Core Files

* `Uniscraper.py`: The main scraper application containing all extraction logic (~5500 lines)
  * **Institution Module**: 80+ functions to extract university-level data (location, contact, demographics, URLs, requirements)
  * **Department Module**: Functions to extract department information from university websites
  * **Graduate Programs Module**: Complete extraction pipeline with multiple sub-stages
  * **Undergraduate Programs Module**: Complete extraction pipeline for undergraduate programs
  * **Merge & Standardization**: Logic to combine and standardize all extracted data into final outputs
* `requirements.txt`: List of Python dependencies (pandas, google-genai, python-dotenv, openpyxl, etc.)
* `.env`: Environment configuration file for API keys and model settings
* `venv/`: Your isolated Python environment (hidden/ignored by git)

### Code Organization in `Uniscraper.py`

The scraper follows a modular design with distinct extraction phases:

1. **Institution Extraction** - Captures university-wide information:
   - Basic details (name, location, street, city, state, country, zip)
   - Contact information (phone, email, website URLs)
   - Academic data (student/faculty ratio, total programs, enrollment numbers)
   - Important URLs (admissions, financial aid, virtual tour, academic calendar, cost of attendance)
   - Application requirements (test policies, recommendations, essays, writing samples)
   - International student requirements

2. **Department Extraction** - Identifies and extracts all departments/schools within the university

3. **Programs Extraction** - Multi-stage pipeline for both graduate and undergraduate programs:
   - **Step 1**: Extract complete list of programs with basic information
   - **Step 2**: Extract extra fields (program URLs, descriptions, specializations)
   - **Step 3**: Extract test score requirements (GRE, GMAT, TOEFL, IELTS, Duolingo, etc.)
   - **Step 4**: Extract application requirements (transcripts, LORs, deadlines)
   - **Step 5**: Extract financial details (tuition, fees, scholarships, assistantships)
   - **Step 6**: Merge and standardize all data into final comprehensive dataset

---

## 📁 Output Folders

When you run the scraper, it automatically creates output directories organized by data type:

### 1. `Inst_outputs/` - Institution Data
Contains university-level information in multiple formats:
- **CSV**: `{UniversityName}_Institution.csv` - Spreadsheet format for data analysis
- **Excel**: `{UniversityName}_Institution.xlsx` - Excel workbook with formatted data
- **JSON**: `{UniversityName}_Institution.json` - Structured data for programmatic use

**Data includes**: University name, location, contact details, student statistics, URLs, and application requirements

### 2. `Dept_outputs/` - Department Data
Contains all departments and schools within the university:
- **CSV**: `{UniversityName}_departments.csv` - List of departments with details
- **JSON**: `{UniversityName}_departments.json` - Department data in JSON format

### 3. `Grad_prog_outputs/` - Graduate Programs Data
Contains comprehensive graduate program information across multiple files:

**Base Data:**
- `{UniversityName}_graduate_programs.csv` - Initial list of all graduate programs
- `{UniversityName}_graduate_programs.json` - Graduate programs in JSON format

**Additional Extractions:**
- `{UniversityName}_extra_fields_data.csv` - Program URLs, descriptions, and additional fields
- `{UniversityName}_test_scores_requirements.csv` - GRE, GMAT, TOEFL, IELTS requirements
- `{UniversityName}_application_requirements.csv` - Application deadlines, transcripts, LORs
- `{UniversityName}_program_details_financial.csv` - Tuition, fees, scholarships, financial aid

**Final Merged Output:**
- `{UniversityName}_graduate_programs_final.csv` - **Complete dataset** with all information merged and standardized

### 4. `Undergrad_prog_outputs/` - Undergraduate Programs Data
Similar structure to graduate programs, containing:
- Base undergraduate program lists
- Extra fields, test requirements, application requirements
- Financial information and scholarships
- Final merged comprehensive dataset

**File naming follows the pattern**: `{UniversityName}_undergraduate_programs_*.csv`

---

## 📊 Data Formats

All output files are generated in multiple formats for flexibility:
- **CSV**: Easy to import into Excel, databases, or data analysis tools
- **JSON**: Structured format ideal for web applications and APIs
- **Excel (XLSX)**: Available for institution data with proper formatting



---

