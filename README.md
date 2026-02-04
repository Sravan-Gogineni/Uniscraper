

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

> **Note:** Replace `main.py` with your actual filename if it differs. If the university name contains spaces, ensure it is wrapped in quotes.

### Example

```bash
python3 Uniscraper.py "Harvard University"

```

---

## 📂 Project Structure

* `main.py`: The entry point of the application.
* `requirements.txt`: List of Python dependencies.
* `venv/`: Your isolated Python environment (hidden/ignored by git).

---

