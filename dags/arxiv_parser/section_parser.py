from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from base_settings.base import (
    DATAFRAMES_PATH,
    default_args,
    databaseConns
)
from base_settings.insert_database_func import  push_df_to_db
import requests
from bs4 import BeautifulSoup
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook


GROBID_URL = "http://grobid:8070/api/processFulltextDocument"

query="""
select
    id,
    pdf_url
from articles a
where a.clean_text is not null and section_text_new is null
limit 10 
"""
# пока что поставил лимит, пока сам не перелью базу в секцию иначе будет фечить все 

def fetch_pdf(pdf_url):
    resp = requests.get(pdf_url, timeout=15)
    resp.raise_for_status()
    return resp.content


def pdf_to_grobid_xml(pdf_bytes, doc_id):
    files = {
        "input": (f"{doc_id}.pdf", pdf_bytes, "application/pdf")
    }
    resp = requests.post(GROBID_URL, files=files, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"GROBID error {resp.status_code}")
    return resp.text


def split_xml_into_sections(xml_text):
    soup = BeautifulSoup(xml_text, "lxml")
    body = soup.find("body")
    if not body:
        return {}

    sections = {}

    for div in body.find_all("div"):
        for tag in div.find_all(["formula", "inline-formula", "figure", "table"]):
            tag.decompose()

        raw = div.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in raw.split("\n") if l.strip()]

        if len(lines) < 2:
            continue

        title = lines[0]
        text = " ".join(lines[1:])
        sections[title] = text

    return sections


def trim_sections_after_conclusion(sections):
    titles = list(sections.keys())
    lowered = [t.lower() for t in titles]

    concl_idx = next((i for i, t in enumerate(lowered) if "conclusion" in t), None)
    ack_idx = next(
        (i for i, t in enumerate(lowered) if "acknowledg" in t or "appendix" in t),
        None,
    )

    if concl_idx is None and ack_idx is None:
        return sections

    cut_idx = min(i for i in [concl_idx, ack_idx] if i is not None)

    return {titles[i]: sections[titles[i]] for i in range(cut_idx + 1)}


def drop_abstract_sections(sections):
    return {k: v for k, v in sections.items() if "abstract" not in k.lower()}


def cut_before_introduction(sections, max_title_words=10):
    result = {}
    started = False

    for title, text in sections.items():
        if "introduction" in title.lower() and len(title.split()) <= max_title_words:
            started = True
        if started:
            result[title] = text

    return result


def drop_non_alpha_sections(sections):
    return {k: v for k, v in sections.items() if re.search(r"[a-zA-Z]", v)}


def merge_short_sections(sections, min_words=30):
    merged = {}
    prev = None

    for title, text in sections.items():
        if len(text.split()) < min_words and prev is not None:
            merged[prev] += " " + text
        else:
            merged[title] = text
            prev = title

    return merged


def merge_short_titles(sections, min_title_len=5, exceptions=None):
    if exceptions is None:
        exceptions = ["Data"]

    merged = {}
    prev = None

    for title, text in sections.items():
        short_title = len(title.strip()) < min_title_len and title not in exceptions
        if short_title and prev is not None:
            merged[prev] += " " + text
        else:
            merged[title] = text
            prev = title

    return merged


def drop_bad_titles(sections, min_title_len=4):
    clean = {}
    for title, text in sections.items():
        if not re.search(r"[a-zA-Z]", title):
            continue
        if len(title.strip()) < min_title_len:
            continue
        clean[title] = text
    return clean


def merge_long_title_sections(sections, max_title_words=30):
    merged = {}
    prev = None

    for title, text in sections.items():
        if len(title.split()) > max_title_words and prev is not None:
            merged[prev] += " " + text
        else:
            merged[title] = text
            prev = title

    return merged


def merge_bad_caption_titles(sections):
    merged = {}
    prev = None

    for title, text in sections.items():
        low = title.lower()
        if (
            "fig" in low
            or "figure" in low
            or "arxiv" in low
            or "theorem" in low
            or "lemma" in low
            or "table" in low
        ) and prev is not None:
            merged[prev] += " " + text
        else:
            merged[title] = text
            prev = title

    return merged


def postprocess_sections(sections, min_words=30):
    sections = drop_abstract_sections(sections)
    sections = cut_before_introduction(sections)
    sections = drop_non_alpha_sections(sections)
    sections = merge_short_sections(sections, min_words)
    sections = merge_long_title_sections(sections)
    sections = merge_bad_caption_titles(sections)
    sections = drop_bad_titles(sections)
    sections = merge_short_titles(sections)
    return sections


def process_row(idx, row):
    try:
        pdf_bytes = fetch_pdf(row["pdf_url"])
        xml_text = pdf_to_grobid_xml(pdf_bytes, row["id"])

        sections = split_xml_into_sections(xml_text)
        sections = trim_sections_after_conclusion(sections)
        sections = postprocess_sections(sections, min_words=40)

        return idx, sections
    except Exception:
        return idx, {}
    
def process_dataframe(query):
    pg_conn = PostgresHook(postgres_conn_id = 'arxiv')
    df = pg_conn.get_pandas_df(query)
    df["section_text_new"] = None
    for idx, row in df.iterrows():
        idx, sections = process_row(idx, row)
        df.at[idx, "section_text_new"] = sections
    filename = f"{DATAFRAMES_PATH}/arxiv_sectioned_text.csv"
    df.drop(columns=['pdf_url'], inplace=True)
    df[df['section_text_new']!={}]
    df.to_csv(filename, index=False)
    return filename

with DAG(
    dag_id="sectioned_text",
    default_args=default_args,
    start_date=datetime(2025,11,10),
    # schedule="0 9 * * *",
    tags=["arxiv"],
    catchup=False, 
) as dag:
    fetch_scraper = PythonOperator(
        task_id = "divide_text_sections",
        python_callable = process_dataframe,
        op_kwargs={
            "query": query,
        }
    )

    insert = PythonOperator(
        task_id = 'insert_section_text',
        python_callable=push_df_to_db,
        op_kwargs={
            "task_id_xcom": "divide_text_sections",
            "key_xcom": "return_value",
            "table_name": "articles",
            "update_cols": ["section_text_new"],
            "conn_id": databaseConns["master"]["postgres_conn_id"],
            "schema_name": databaseConns["master"]["schema"],
            "index_cols": ["id"],
        }
    )
    fetch_scraper >> insert
