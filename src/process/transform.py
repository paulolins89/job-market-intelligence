"""
Phase 2 PySpark processing pipeline.

Reads raw JSON files from data/raw/, cleans and standardises the records,
extracts skills, and writes two parquet files to data/processed/:
  - jobs.parquet       one row per unique job posting
  - job_skills.parquet one row per job-skill pair (for aggregations)
"""

import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parents[2]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"

# ---------------------------------------------------------------------------
# Skills taxonomy
# ---------------------------------------------------------------------------

SKILLS: dict[str, list[str]] = {
    "languages": ["python", "sql", "r", "scala", "java", "julia", "go", "bash"],
    "bi_tools": ["power bi", "tableau", "looker", "qlik", "metabase", "superset"],
    "cloud": ["aws", "azure", "gcp", "google cloud", "databricks", "snowflake"],
    "processing": ["spark", "pyspark", "kafka", "airflow", "dbt", "luigi", "flink"],
    "databases": ["postgres", "postgresql", "mysql", "mongodb", "redis", "elasticsearch", "bigquery", "redshift"],
    "ml": ["machine learning", "deep learning", "scikit-learn", "tensorflow", "pytorch", "keras", "xgboost"],
    "practices": ["etl", "elt", "a/b testing", "statistics", "data modeling", "data warehousing", "ci/cd", "docker", "kubernetes", "git"],
    "excel": ["excel", "vba"],
}

# Flat list of (skill_term, category) for matching
_SKILL_TERMS: list[tuple[str, str]] = [
    (skill, category)
    for category, skills in SKILLS.items()
    for skill in skills
]


def _build_extract_udf():
    """
    Returns a Spark UDF that takes a pipe-delimited skill string (from
    technical_tools) or a plain text description and returns a list of
    matched skill names from the taxonomy.
    """
    skill_terms = _SKILL_TERMS  # captured in closure

    def extract_skills(text: str) -> list[str]:
        if not text:
            return []
        text_lower = text.lower()
        matched = []
        for skill, _ in skill_terms:
            # word-boundary check: skill must not be surrounded by word chars
            pattern = r"(?<![a-z0-9])" + re.escape(skill) + r"(?![a-z0-9])"
            if re.search(pattern, text_lower):
                matched.append(skill)
        return matched

    return F.udf(extract_skills, ArrayType(StringType()))


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def load_raw(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """Load all raw JSON files, extracting the jobs array from each."""
    json_files = list(RAW_DIR.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {RAW_DIR}")

    print(f"Loading {len(json_files)} raw file(s)...")

    # Each file is a JSON object with a top-level 'jobs' array.
    # spark.read.json can read the outer object; we then select jobs[*].
    df = spark.read.option("multiLine", True).json([str(p) for p in json_files])
    df = df.select(F.explode("jobs").alias("job"))
    df = df.select("job.*")

    return df


def clean(df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """Flatten nested fields, deduplicate, standardise types."""

    df = df.select(
        F.col("id"),
        F.col("job_information.title").alias("title"),
        F.col("job_information.job_title_raw").alias("title_raw"),
        F.col("job_information.description").alias("description_html"),
        # v5 processed fields
        F.col("v5_processed_job_data.core_job_title").alias("core_job_title"),
        F.col("v5_processed_job_data.technical_tools").alias("technical_tools"),
        F.col("v5_processed_job_data.seniority_level").alias("seniority_level"),
        F.col("v5_processed_job_data.role_type").alias("role_type"),
        F.col("v5_processed_job_data.job_category").alias("job_category"),
        F.col("v5_processed_job_data.commitment").alias("commitment"),
        F.col("v5_processed_job_data.workplace_type").alias("workplace_type"),
        F.col("v5_processed_job_data.workplace_countries").alias("workplace_countries"),
        F.col("v5_processed_job_data.workplace_cities").alias("workplace_cities"),
        F.col("v5_processed_job_data.formatted_workplace_location").alias("location"),
        F.col("v5_processed_job_data.yearly_min_compensation").alias("salary_min"),
        F.col("v5_processed_job_data.yearly_max_compensation").alias("salary_max"),
        F.col("v5_processed_job_data.listed_compensation_currency").alias("salary_currency"),
        F.col("v5_processed_job_data.estimated_publish_date").alias("posted_date_raw"),
        F.col("v5_processed_job_data.visa_sponsorship").alias("visa_sponsorship"),
        # company fields
        F.col("enriched_company_data.name").alias("company_name"),
        F.col("enriched_company_data.hq_country").alias("company_hq_country"),
        F.col("enriched_company_data.industries").alias("company_industries"),
        F.col("enriched_company_data.nb_employees").alias("company_size"),
        F.col("apply_url"),
        F.col("source"),
    )

    # Deduplicate on job id — keep first occurrence across files/queries
    df = df.dropDuplicates(["id"])

    # Parse posted date to date type
    df = df.withColumn(
        "posted_date",
        F.to_date(F.col("posted_date_raw")),
    ).drop("posted_date_raw")

    # Add ingestion date (today's run)
    df = df.withColumn("ingestion_date", F.current_date())

    # Strip HTML tags from description (simple regex — good enough for this data)
    df = df.withColumn(
        "description",
        F.regexp_replace(F.col("description_html"), r"<[^>]+>", " "),
    ).withColumn(
        "description",
        F.regexp_replace(F.col("description"), r"\s{2,}", " "),
    ).withColumn(
        "description",
        F.trim(F.col("description")),
    ).drop("description_html")

    # Cast salary columns to double (they may come in as string or long)
    df = df.withColumn("salary_min", F.col("salary_min").cast("double"))
    df = df.withColumn("salary_max", F.col("salary_max").cast("double"))

    return df


def extract_skills(df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    Add a 'matched_skills' array column using the taxonomy UDF.
    technical_tools is an array — join to string first for uniform matching.
    """
    extract_udf = _build_extract_udf()

    # Convert technical_tools array to a space-delimited string for matching
    df = df.withColumn(
        "tools_str",
        F.concat_ws(" | ", F.col("technical_tools")),
    )
    df = df.withColumn("matched_skills", extract_udf(F.col("tools_str"))).drop("tools_str")
    return df


def build_job_skills(df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    Explode matched_skills so each job-skill pair is its own row.
    Also joins category back from the taxonomy.
    """
    skill_to_category = {skill: cat for cat, skills in SKILLS.items() for skill in skills}
    map_expr = F.create_map(
        *[item for pair in skill_to_category.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )

    job_skills = (
        df.select("id", "title", "posted_date", "workplace_type", "salary_min", "salary_max", F.explode("matched_skills").alias("skill"))
        .withColumn("skill_category", map_expr[F.col("skill")])
    )
    return job_skills


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("job-market-intelligence")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")  # local mode — no need for 200
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2.1 Load
    df_raw = load_raw(spark)

    # 2.2 Clean
    df_clean = clean(df_raw)

    # 2.3 Skill extraction
    df_clean = extract_skills(df_clean)

    # 2.4 Write parquet
    jobs_out = str(PROCESSED_DIR / "jobs.parquet")
    skills_out = str(PROCESSED_DIR / "job_skills.parquet")

    df_clean.drop("matched_skills").write.mode("overwrite").parquet(jobs_out)
    print(f"Written: {jobs_out}")

    df_skills = build_job_skills(df_clean)
    df_skills.write.mode("overwrite").parquet(skills_out)
    print(f"Written: {skills_out}")

    # Sanity check from parquet (cheap re-read)
    jobs_check = spark.read.parquet(jobs_out)
    print(f"\nTotal unique jobs: {jobs_check.count():,}")
    jobs_check.select("id", "title", "posted_date", "workplace_type", "salary_min", "salary_max").show(5, truncate=80)

    skills_check = spark.read.parquet(skills_out)
    print("\nTop 20 skills:")
    skills_check.groupBy("skill").count().orderBy(F.desc("count")).show(20, truncate=False)

    spark.stop()
    print("\nDone. Phase 2 complete.")


if __name__ == "__main__":
    main()
