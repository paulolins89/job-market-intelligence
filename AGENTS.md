__Job Market Intelligence Pipeline__

*A hands\-on project to learn PySpark, SQL, and Power BI on data that actually matters to you\.*

# What You're Building

A data pipeline that ingests job postings from hiring\.cafe, processes them with PySpark, stores them in a DuckDB database, and surfaces insights in a Power BI dashboard — answering questions like: which skills appear most in data analyst roles in the Netherlands? How has Python vs SQL demand shifted over time? What salary ranges are realistic for your profile?

This is also a genuine job search tool\. Every run gives you fresher intelligence on the market you're targeting\.

# Tech Stack

- __Python 3\.11\+__  —  ingestion, scraping, orchestration
- __PySpark \(local mode\)__  —  processing, transformations, skill extraction
- __DuckDB__  —  SQL storage layer, queryable from Python and Power BI
- __Power BI Desktop \(free\)__  —  dashboard and visualisation
- __Git \+ GitHub__  —  version control and portfolio visibility

# Repository Structure

Set this up before writing a single line of code\. Having a clean structure from day one prevents the chaos of files scattered everywhere\.

__job\-market\-intelligence/__

├── data/

│   ├── raw/          ← raw JSON from hiring\.cafe

│   ├── processed/    ← parquet files from PySpark

│   └── db/           ← DuckDB database file

├── src/

│   ├── ingest/       ← scraper and data fetching

│   ├── process/      ← PySpark transformation logic

│   └── load/         ← DuckDB write logic

├── notebooks/        ← exploratory work \(Jupyter\)

├── sql/              ← SQL queries and schema definitions

├── powerbi/          ← Power BI \.pbix file

├── tests/            ← basic unit tests

├── \.env              ← config \(gitignored\)

├── requirements\.txt

└── README\.md

 

__PHASE 0  Environment Setup__*   ·   est\. 2–3 hours*

Do this entirely before opening Claude Code\. A working environment means you can focus on building, not troubleshooting installs mid\-session\.

## 0\.1  Python environment

- Install pyenv to manage Python versions cleanly on Linux
- curl https://pyenv\.run | bash  — then follow the shell config instructions it outputs
- Install Python 3\.11: pyenv install 3\.11\.9 && pyenv global 3\.11\.9
- Create a virtual environment inside the project folder:
- python \-m venv \.venv && source \.venv/bin/activate
- Add \.venv/ to \.gitignore immediately

## 0\.2  Core dependencies

Install these into your venv and pin them in requirements\.txt:

- pyspark  —  confirm with: python \-c "import pyspark; print\(pyspark\.\_\_version\_\_\)"
- duckdb  —  confirm with a simple connect \+ query in Python
- requests, beautifulsoup4  —  for the scraper
- python\-dotenv  —  for managing config cleanly
- jupyter  —  for exploratory work before wiring things into scripts
- Java \(JDK 11\)  —  PySpark requires it\. Install via: sudo apt install openjdk\-11\-jdk

## 0\.3  Git setup

- Create a GitHub repo named job\-market\-intelligence \(public — it's a portfolio piece\)
- Clone it locally, set up the folder structure from the diagram above
- Create a \.gitignore that excludes: \.venv/, data/raw/, data/db/, \.env
- Make an initial commit with just the README and folder structure

## 0\.4  Phase 0 done when\.\.\.

- python \-\-version returns 3\.11\.x
- import pyspark works inside your venv
- import duckdb; con = duckdb\.connect\(\) works
- Repo exists on GitHub with folder structure committed
- __Do not proceed to Phase 1 until all of these pass__

__PHASE 1  Data Ingestion__*   ·   est\. 8–12 hours*

Goal: get raw job posting data onto disk as JSON\. Nothing fancy yet — just get real data you can work with\.

## 1\.1  Explore the hiring\.cafe API manually

- Open hiring\.cafe in a browser, open DevTools \(F12\) → Network tab
- Search for "data analyst Netherlands" and watch the network requests
- Identify the API endpoint being called — note the URL pattern, query params, and headers
- Copy the request as cURL and test it in terminal to confirm it works
- __Document the API structure: endpoint, pagination pattern, response fields\. Write this in a markdown file before coding\.__

## 1\.2  Build the scraper \(src/ingest/scraper\.py\)

- Write a function fetch\_jobs\(query, location, max\_pages\) that hits the API and returns raw JSON
- Add a polite delay between requests \(time\.sleep\(1–2 seconds\)\) — don't hammer the server
- Handle errors gracefully: timeouts, empty pages, malformed responses — log them, don't crash
- Save raw output to data/raw/ as JSON with a timestamp in the filename \(jobs\_20250401\.json\)
- Run it for your target searches: "data analyst", "business analyst", "python developer" — Netherlands focus

## 1\.3  Explore the raw data

- Open a Jupyter notebook in notebooks/ and load the raw JSON
- Print field names — what does each record actually contain?
- Check for nulls, inconsistent formats, nested structures that need flattening
- __Look at 10–20 raw records manually\. This is your data — know what's in it before building a pipeline\.__
- Write down in a comment or markdown file: what fields are clean, what needs fixing, what's missing

## 1\.4  Phase 1 done when\.\.\.

- Scraper runs without crashing and produces at least 500 job records
- Raw JSON files are saved to data/raw/ with timestamps
- You have manually looked at the data and written down what needs to be cleaned

__PHASE 2  PySpark Processing Pipeline__*   ·   est\. 15–20 hours*

Goal: transform messy raw JSON into clean, structured, queryable data\. This is where PySpark earns its place — and where the real learning happens\.

## 2\.1  Load raw JSON into a Spark DataFrame

- Create a SparkSession in local mode \(no cluster needed\): SparkSession\.builder\.master\("local\[\*\]"\)\.\.\.
- Load JSON with spark\.read\.json\(path\) — Spark infers schema automatically
- Print the schema with df\.printSchema\(\) — confirm it matches what you saw in the notebook
- df\.show\(5\) and df\.count\(\) — sanity check before doing anything else

## 2\.2  Clean and standardise

- Drop duplicate records \(same job ID appearing across multiple scrape runs\)
- Standardise location fields — trim whitespace, normalise country/city names
- Parse salary fields where present — extract min/max/currency as separate numeric columns
- Standardise date fields to a consistent format \(ISO 8601\)
- Strip HTML tags from job descriptions where they appear
- Add an ingestion\_date column so you can track when each record was collected

## 2\.3  Skill extraction \(the interesting part\)

This is the core analytical logic of the project\. The goal is to scan job descriptions and tag each job with the skills it mentions\.

- Start with a curated skills list — a Python dict of categories and terms:
- Languages: python, sql, r, scala, java
- BI tools: power bi, tableau, looker, qlik
- Cloud/infra: aws, azure, gcp, databricks, snowflake
- Processing: spark, pyspark, kafka, airflow, dbt
- Practices: etl, machine learning, a/b testing, statistics
- Write a Spark UDF that takes a job description string and returns a list of matched skills
- Use case\-insensitive matching and word boundary checks \(avoid "scala" matching "scalable"\)
- Explode the skills list so each job\-skill combination is its own row — this is what enables aggregations
- __Spot\-check 20–30 records manually\. Does the extraction look right? Fix obvious errors in your skills list\.__

## 2\.4  Write clean output to parquet

- Write the cleaned jobs DataFrame to data/processed/jobs\.parquet
- Write the exploded skills DataFrame to data/processed/job\_skills\.parquet
- *Parquet is columnar — DuckDB reads it natively and very fast\. This is the bridge between PySpark and SQL\.*

## 2\.5  Phase 2 done when\.\.\.

- Pipeline runs end\-to\-end: raw JSON in → clean parquet out
- jobs\.parquet and job\_skills\.parquet exist in data/processed/
- You can query the parquet with DuckDB and get sensible results: SELECT skill, COUNT\(\*\) FROM \.\.\. GROUP BY skill

__PHASE 3  SQL Layer \(DuckDB\)__*   ·   est\. 6–10 hours*

Goal: build a proper data model in DuckDB that Power BI can connect to — and that you can query directly to answer real questions\.

## 3\.1  Design the schema \(do this on paper first\)

__Think about the questions you want to answer before writing any SQL:__

- Which skills are most in demand for data analyst roles in NL?
- How does skill demand compare across role types \(analyst vs engineer vs scientist\)?
- What salary ranges are attached to roles that require Python vs Power BI vs SQL?
- Which companies are hiring most actively right now?
- How many roles list remote/hybrid vs on\-site?

## 3\.2  Create the tables in DuckDB

Write these DDL statements in sql/schema\.sql — run them once to set up the database:

- dim\_jobs — one row per job posting \(id, title, company, location, remote\_type, salary\_min, salary\_max, posted\_date, ingestion\_date\)
- dim\_skills — reference table of all known skills and their categories
- fact\_job\_skills — bridge table: job\_id \+ skill\_name \(one row per job\-skill pair\)

## 3\.3  Load parquet into DuckDB

- DuckDB can read parquet directly: INSERT INTO dim\_jobs SELECT \* FROM read\_parquet\('data/processed/jobs\.parquet'\)
- Write a load script \(src/load/load\_db\.py\) that handles this programmatically
- Add a check: skip records already in the database \(avoid duplicates on repeated runs\)

## 3\.4  Write and save your analytical queries

Save every useful query as a \.sql file in sql/queries/\. These become your analytical library\.

- top\_skills\_by\_role\.sql — GROUP BY role type, rank skills by frequency
- salary\_by\_skill\.sql — AVG salary where skill is present vs absent
- hiring\_volume\_by\_company\.sql — which companies are posting most
- remote\_vs\_onsite\.sql — split by work type

## 3\.5  Phase 3 done when\.\.\.

- DuckDB file exists at data/db/jobs\.duckdb
- All three tables are populated with real data
- You can answer at least 3 of the questions from 3\.1 with SQL queries that return sensible results

__PHASE 4  Power BI Dashboard__*   ·   est\. 8–12 hours*

Goal: a dashboard that answers your real questions visually — and that you can screenshot for your portfolio\.

## 4\.1  Connect Power BI to DuckDB

- Install the DuckDB ODBC driver for Windows \(Power BI Desktop runs on Windows\)
- Alternative if ODBC is painful: export DuckDB tables to CSV/parquet and connect Power BI to those files
- Import mode is fine for this project — no need for DirectQuery

## 4\.2  Build these pages \(one per question\)

- Page 1 — Skills Overview: horizontal bar chart, top 20 skills by frequency, filterable by role type
- Page 2 — Salary Intelligence: box plot or bar chart of salary ranges by skill / role type
- Page 3 — Market Activity: hiring volume by company, postings over time, remote vs on\-site split
- Page 4 — My Profile Fit: a custom view showing how your skills stack up against what the market is asking for

## 4\.3  Polish before declaring done

- Consistent colour scheme throughout — pick one and stick to it
- Every chart has a clear title and labelled axes
- At least one cross\-page slicer \(e\.g\. role type or date range\) that actually does something useful
- Take screenshots of each page — these go in the README

## 4\.4  Phase 4 done when\.\.\.

- 4 dashboard pages exist and load without errors
- You can answer all the questions from Phase 3 visually
- Screenshots taken and ready for the README

__PHASE 5  Portfolio Polish__*   ·   est\. 3–5 hours*

__This phase is what separates a project that exists from one that demonstrates capability\. Don't skip it\.__

## 5\.1  README\.md — the most important file in the repo

- Opening paragraph: what problem does this solve and why did you build it?
- Architecture diagram or simple text overview of the pipeline
- Screenshots of the Power BI dashboard
- Key findings section — 3–5 actual insights the data produced \(e\.g\. "SQL appears in 78% of NL data analyst postings; Power BI appears in 61%"\)
- Tech stack listed clearly
- Setup instructions — how to run the pipeline from scratch

## 5\.2  Code quality

- Every function has a docstring explaining what it does and what it returns
- No dead code, no commented\-out blocks left in
- Run ruff on the codebase and fix what it flags

## 5\.3  What to say about this project in an interview

__Prepare answers to these questions before you start applying:__

- "Why did you build this?" — you were actively job searching and wanted market intelligence on what skills were actually in demand vs what you assumed\.
- "What was the hardest part?" — skill extraction from free\-text job descriptions: false positives, boundary matching, evolving your skill taxonomy based on what the data showed\.
- "What did you learn?" — be specific\. PySpark local mode for the processing logic\. DuckDB as a lightweight analytical database\. Power BI for surfacing the results\.

__Rules for Claude Code Sessions__

__Read these before every coding session\.__

1. __One phase at a time\. Do not start Phase 2 until Phase 1 is fully done\.__
2. __One task at a time within a phase\. Complete the task, verify it works, commit, then move on\.__
3. __Commit after every completed task\. Small commits\. If something breaks, you can always roll back\.__
4. __Before starting a session: re\-read the phase you're on and identify exactly which task you're doing today\.__
5. __If you get off track \(rabbit holes, scope creep, "just one more feature"\): stop, come back here, find your task\.__
6. __Do not skip the "done when" checklist at the end of each phase\.__
7. __If you're stuck on something for more than 30 minutes, write down what you're trying to do and ask for help\. Don't tunnel\.__

Total estimate: 40–60 hours   ·   At 5 hrs/week: 8–12 weeks   ·   At 10 hrs/week: 4–6 weeks

