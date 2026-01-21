# src/pipeline.py
from dagster import job, op, repository, schedule, AssetIn, Out
import subprocess
from dagster_dbt import DbtCliResource


@op
def scrape_telegram_data(context):
    subprocess.run(["python", "src/scraper.py"], check=True)
    context.log.info("Scraping complete")


@op
def load_raw_to_postgres(context):
    subprocess.run(["python", "scripts/load_raw.py"], check=True)
    context.log.info("Raw load complete")


@op
def run_dbt_transformations(context, dbt: DbtCliResource):
    result = dbt.cli.execute(["run"], target="dev")
    if result.return_code != 0:
        raise Exception("dbt run failed")
    context.log.info("dbt transformations complete")


@op
def run_yolo_enrichment(context):
    subprocess.run(["python", "src/yolo_detect.py"], check=True)
    subprocess.run(["python", "scripts/load_yolo.py"], check=True)
    context.log.info("YOLO enrichment complete")


@job
def medical_pipeline():
    dbt = DbtCliResource(project_dir="../medical_warehouse")
    scrape = scrape_telegram_data()
    load_raw = load_raw_to_postgres(scrape)
    dbt_transform = run_dbt_transformations(load_raw, dbt=dbt)
    yolo = run_yolo_enrichment(dbt_transform)


@schedule(job=medical_pipeline, cron_schedule="0 0 * * *")  # Daily at midnight
def daily_schedule():
    return {}


@repository
def medical_repo():
    return [medical_pipeline, daily_schedule]
