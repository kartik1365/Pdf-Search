import os
import json
import pdfplumber
import pandas as pd
import logging
import ssl
from elasticsearch import Elasticsearch, helpers

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SSL context for Elasticsearch (disable verification for local/testing)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Elasticsearch client
es = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=('elastic', 'LH3PU-tMng*hkk0TVfwN'),
    ssl_context=ssl_context,
    verify_certs=False,
    ssl_show_warn=False
)

INDEX_NAME = os.getenv('ELASTIC_INDEX', 'pdf_data')

# --- Analyzer & Mapping for Partial Search ---
def reinit_index():
    ngram_mapping = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "edge_ngram_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "edge_ngram_filter"]
                    }
                },
                "filter": {
                    "edge_ngram_filter": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 15
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "pdf_id":         { "type": "keyword" },
                "type":           { "type": "keyword" },
                "page":           { "type": "integer" },
                "content":        { "type": "text", "analyzer": "edge_ngram_analyzer", "search_analyzer": "standard" },
                "caption":        { "type": "text", "analyzer": "edge_ngram_analyzer", "search_analyzer": "standard" },
                "image_metadata": { "type": "text", "analyzer": "edge_ngram_analyzer", "search_analyzer": "standard" }
            }
        }
    }
    if es.indices.exists(index=INDEX_NAME):
        logger.info(f"Deleting index: {INDEX_NAME}")
        es.indices.delete(index=INDEX_NAME)
    logger.info(f"Creating index: {INDEX_NAME} with partial search analyzer")
    es.indices.create(index=INDEX_NAME, body=ngram_mapping)

# --- Extraction and Transformation utilities as before ---

def make_unique(headers):
    seen = {}
    result = []
    for idx, h in enumerate(headers):
        if isinstance(h, (str, int, float)):
            name = str(h).strip()
        else:
            name = f"col_{idx}"
        if not name:
            name = f"col_{idx}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        result.append(name)
    return result

def extract_tables(pdf_path):
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            page_tables = page.extract_tables()
            for i, table in enumerate(page_tables):
                if len(table) > 1 and table[0]:
                    headers = make_unique(table[0])
                    clean_rows = []
                    for row in table[1:]:
                        if len(row) < len(headers):
                            row += [None] * (len(headers) - len(row))
                        elif len(row) > len(headers):
                            row = row[:len(headers)]
                        clean_rows.append(row)
                    df = pd.DataFrame(clean_rows, columns=headers)
                else:
                    df = pd.DataFrame(table)
                tables.append({
                    "page": page_num + 1,
                    "table_index": i,
                    "table_data": df.to_dict(orient='records')
                })
    return tables

def extract_paragraphs(pdf_path):
    paragraphs = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
            paras = [p.strip() for p in text.split('\n\n') if p.strip()]
            for i, p in enumerate(paras):
                paragraphs.append({
                    "page": page_num + 1,
                    "paragraph_index": i,
                    "text": p
                })
    return paragraphs

def make_json_serializable(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items() if k != 'stream'}
    elif isinstance(obj, list):
        return [make_json_serializable(i) for i in obj]
    else:
        return str(obj)

def extract_images(pdf_path):
    images = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            for i, img in enumerate(page.images):
                try:
                    clean_meta = make_json_serializable(img)
                    images.append({
                        "page": page_num + 1,
                        "image_index": i,
                        "image_metadata": clean_meta,
                        "caption": ""
                    })
                except Exception as e:
                    logger.warning(f"Failed to process image metadata on page {page_num + 1} img_index {i}: {e}")
    return images

def generate_actions(pdf_id, pdf_path):
    paragraphs = extract_paragraphs(pdf_path)
    tables = extract_tables(pdf_path)
    images = extract_images(pdf_path)

    for p in paragraphs:
        yield {
            "_index": INDEX_NAME,
            "_id": f"{pdf_id}_para_{p['page']}_{p['paragraph_index']}",
            "_source": {
                "pdf_id": pdf_id,
                "type": "paragraph",
                "page": p['page'],
                "content": p['text']
            }
        }

    for t in tables:
        table_text = '\n'.join(
            [', '.join([str(cell) if cell is not None else '' for cell in row.values()]) for row in t['table_data']]
        )
        yield {
            "_index": INDEX_NAME,
            "_id": f"{pdf_id}_table_{t['page']}_{t['table_index']}",
            "_source": {
                "pdf_id": pdf_id,
                "type": "table",
                "page": t['page'],
                "content": table_text
            }
        }

    for img in images:
        yield {
            "_index": INDEX_NAME,
            "_id": f"{pdf_id}_img_{img['page']}_{img['image_index']}",
            "_source": {
                "pdf_id": pdf_id,
                "type": "image",
                "page": img['page'],
                "image_metadata": img['image_metadata'],
                "caption": img['caption']
            }
        }

def transform_and_index(pdf_id, pdf_path):
    try:
        reinit_index()  # <--- Add this at the start!
        actions = generate_actions(pdf_id, pdf_path)
        helpers.bulk(es, actions)
        logger.info(f"Indexed {pdf_id} successfully")
    except Exception as e:
        logger.error(f"Failed to index {pdf_id}: {e}")

def main():
    pdf_folder = '.'
    for file in os.listdir(pdf_folder):
        if file.endswith('.pdf'):
            pdf_id = os.path.splitext(file)[0]
            pdf_path = os.path.join(pdf_folder, file)
            logger.info(f"Processing {pdf_path}")
            transform_and_index(pdf_id, pdf_path)

if __name__ == "__main__":
    try:
        info = es.info()
        logger.info(f"Connected to Elasticsearch: {info}")
    except Exception as e:
        logger.error(f"Connection failed: {e}")
    main()
