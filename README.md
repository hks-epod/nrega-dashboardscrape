# About this project
This was designed to scrape various NREGA related stats at the GP level and chart them. The codebase is in Python and was initially authored by Mehmet Seflek (EPoD Cambridge).

# Description of code
**cr_web_extract.py**: scrapes data from the NREGA backend and creates a dataframe. Errors are captured and handled in cr_web_retry.py
 
**cr_web_retry.py**: retries states and GPs that failed in cr_web_extract.py

**an_dashboard_scrape_charting.py**: basic analysis of the scraped data, primarily focused on delay days.

# Timing
Since the datasource is updated often, I suggest running the following programs in the following frequency:

**cr_web_extract.py**: Weekly

**cr_web_retry.py**: Daily 

**an_dashboard_scrape_charting.py**: Whenever an analysis is needed

# Updates needed/issues

The link between **cr_web_extract.py** and **cr_web_retry.py** is not foolproof right now. You may end up with duplicates.
