import arxivscraper
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time
import json
import arxiv
from base_settings.base import (
    DATAFRAMES_PATH,
    default_args,
    databaseConns
)
from base_settings.upgraded_postgreshook import  push_df_to_db
import requests
from bs4 import BeautifulSoup
import fitz
import re

GROBID_URL = "http://grobid:8070/api/processFulltextDocument"

HEADINGS = [
    "references", "bibliography", "literature cited",
    "works cited", "список литературы", "библиография", "references:"
]
PATTERN = re.compile(r"\b\d{4}\.\d{5}(?:v\d+)?\b")

# workflow test
TIMEOUT = 30
REQUEST_SLEEP = 2
BATCH = 20
category = "cs"
#date_start = '2025-11-18'
#date_end = '2025-11-19'

date_end = datetime.today().strftime('%Y-%m-%d')
date_start = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

def extract_refs_from_pdf(pdf_url, arxiv_id):
    print(f"[{arxiv_id}] ЗоГрУзКа")
    try:
        pdf_bytes = requests.get(pdf_url, timeout=TIMEOUT).content
    except Exception as e:
        print(f"[{arxiv_id}] Не загрузился {e}")
        return None

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        print(f"[{arxiv_id}] Не открылся {e}")
        return None

    pages = [
        doc.load_page(i).get_text("text") 
        if i < doc.page_count else ""
        for i in range(doc.page_count)
    ]

    lower = [p.lower() for p in pages]
    start = next(
        (i for i, p in enumerate(lower) if any(h in p for h in HEADINGS)),
        None
    )

    text = "\n".join(pages[start:] if start is not None else pages)

    matches = [re.sub(r"\s+", "", m) for m in PATTERN.findall(text)]
    print(f"[{arxiv_id}] refs found: {len(matches)}")
    return matches if matches else None


def clean_pdf_text(pdf_url, arxiv_id="paper"):
    try:
        print(f"[{arxiv_id}] Загрузка пидиди")
        r = requests.get(pdf_url, timeout=10)
        pdf_bytes = r.content

        print(f"[{arxiv_id}] Ссылка в гробик")
        files = {"input": (f"{arxiv_id}.pdf", pdf_bytes, "application/pdf")}
        r2 = requests.post(GROBID_URL, files=files, timeout=60)
        if r2.status_code != 200:
            print(f"[{arxiv_id}] GROBID returned status {r2.status_code}")
            return ""

        print(f"[{arxiv_id}] Рассмотр xml")
        soup = BeautifulSoup(r2.text, "lxml")
        for tag in soup.find_all([
            "abstract","formula","inline-formula","figure","table",
            "ref","biblStruct","title","author","persName","affiliation",
            "editor","idno"
        ]):
            tag.decompose()

        body = soup.find("body")
        if not body:
            print(f"[{arxiv_id}] Плохо спарсился")
            return ""

        print(f"[{arxiv_id}] Извлечение")
        texts = []
        for div in body.find_all("div"):
            if div.get("type") == "references":
                continue
            for t in div.find_all(["figure","table","formula","ref","biblStruct"]):
                t.decompose()
            text = div.get_text(separator=" ", strip=True)
            if text:
                texts.append(text)

        clean_text = " ".join(texts)
        return clean_text

    except Exception as e:
        print(f"[{arxiv_id}] Абоба {e}")
        return ""

def fetch_metadata(ti, category, date_start, date_end):
    scraper = arxivscraper.Scraper(category=category, date_from=date_start, date_until=date_end)

    output = scraper.scrape()
    print(output)
    df = pd.DataFrame(output)
    df = df[["id", "title", "abstract", "categories", "created", "authors"]]

    df = df.rename(columns={"created": "published"})

    df["pdf_url"] = df["id"].apply(lambda x: f"https://export.arxiv.org/pdf/{x}.pdf")
    df['id'] = df['id'].astype(str)
    df['authors'] = df['authors'].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
    df['published'] = pd.to_datetime(df['published']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df[df['id'].notna()]
    df[df['id'].notna() & (df['id'] != '')]
    filename = f"{DATAFRAMES_PATH}/test.csv"
    df.to_csv(filename, index=False)

    return filename


def fetch_arxiv_metadata(ti, task_id_xcom, key_xcom, batch_size):
    filename = ti.xcom_pull(task_ids=task_id_xcom, key=key_xcom)
    df = pd.read_csv(filename)
    comments = {}
    df['id'] = df['id'].astype(str)
    ids = df['id'].unique().tolist()
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i+batch_size]
        try:
            search = arxiv.Search(id_list=batch_ids)
            for article in search.results():
                id = article.get_short_id()
                comments[id] = article.comment
        except arxiv.HTTPError as e:
            time.sleep(REQUEST_SLEEP)
    df_comments = pd.DataFrame({"id": list(comments.keys()), "comment": list(comments.values())})
    df_comments['id'] = df_comments['id'].str.replace(r'v.*$', '',regex=True)
    df = df.merge(df_comments, how='left', on='id')
#    df['num_pages'] = df['comment'].apply(extract_num_pages)
    filename = f"{DATAFRAMES_PATH}/arxiv_metadata.csv"
    df.to_csv(filename, index=False)
    return filename

def test(ti, task_id_xcom, key_xcom):
    filename = ti.xcom_pull(task_ids=task_id_xcom, key=key_xcom)
    df = pd.read_csv(filename)
    df["clean_text"] = df.apply(lambda row: clean_pdf_text(row["pdf_url"], row["id"]), axis=1)
    df["references_id"] = df.apply(lambda row: extract_refs_from_pdf(row["pdf_url"], row["id"]), axis=1)
    print(df)
    print(df['clean_text'])
    df = df[df['clean_text'] != '']
    filename = f"{DATAFRAMES_PATH}/arxiv_clean_text_refs.csv"
    df.to_csv(filename, index=False)
    return filename

with DAG(
    dag_id = "parser_test",
    default_args=default_args,
    start_date=datetime(2025,11,10),
    schedule="0 9 * * *",
    tags=["arxiv"],
    catchup=False, 
) as dag:
    fetch_scraper = PythonOperator(
        task_id = "fetch_scraper_metadata",
        python_callable = fetch_metadata,
        op_kwargs={
            "category": category,
            "date_start": date_start,
            "date_end": date_end,
        }
    )
    fetch_arxiv = PythonOperator(
        task_id = "fetch_arxiv_metadata",
        python_callable=fetch_arxiv_metadata,
        op_kwargs={
            "task_id_xcom": "fetch_scraper_metadata",
            "key_xcom": "return_value",
            "batch_size": BATCH,
        }
    )
    test = PythonOperator(
        task_id='clean_text',
        python_callable=test,
        op_kwargs={
            "task_id_xcom": "fetch_arxiv_metadata",
            "key_xcom": "return_value",
        }
    )
    insert = PythonOperator(
        task_id = 'insert_arxiv_metadata',
        python_callable=push_df_to_db,
        op_kwargs={
            "task_id_xcom": "clean_text",
            "key_xcom": "return_value",
            "table_name": "articles",
            "update_cols": ["title", "abstract", "categories", "published", "authors", "clean_text", "references_id"],
            "conn_id": databaseConns["master"]["postgres_conn_id"],
            "schema_name": databaseConns["master"]["schema"],
            "index_cols": ["id"],
        }
    )
    fetch_scraper >> fetch_arxiv >> test >> insert

