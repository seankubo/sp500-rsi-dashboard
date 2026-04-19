# Test Procedure

## 1) Install dependencies

```powershell
.\venv\Scripts\python -m pip install -r requirements.txt
```

## 2) Run automated tests

```powershell
.\venv\Scripts\python -m pytest -q
```

## 3) Run the Streamlit dashboard manually

```powershell
.\venv\Scripts\python -m streamlit run app.py
```

Expected behavior:
- Page title is **S&P 500 RSI Dashboard**
- One stock section appears for `^GSPC`
- Hovering over chart points shows date and RSI details
