from fastapi import FastAPI, Depends, HTTPException, Security, Request, File, UploadFile, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional, List
from elasticsearch import Elasticsearch
from pydantic import BaseModel, Field
import ssl
import os
import shutil
import asyncio
import logging
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

from etl_pipeline import transform_and_index  # Your ETL indexing function module

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

PDF_STORAGE_DIR = "./uploaded_pdfs"
os.makedirs(PDF_STORAGE_DIR, exist_ok=True)

# SSL context for Elasticsearch with certificate verification
ssl_context = ssl.create_default_context(cafile="cert_file.pem")
ssl_context.check_hostname = True
ssl_context.verify_mode = ssl.CERT_REQUIRED

class ContentSearchRequest(BaseModel):
    query: str = Field(..., max_length=256)
    size: Optional[int] = Field(10, ge=1, le=100)
    exact_match: Optional[bool] = False

class ContentSearchResult(BaseModel):
    doc_id: str
    type: str
    page: int
    content: Optional[str] = None
    
# Elasticsearch client config
es = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=("elastic", "LH3PU-tMng*hkk0TVfwN"),
    ssl_context=ssl_context,
    verify_certs=True,
)

# Rate Limiter setup (100 requests per IP per minute)
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)

# API Token for securing search endpoint
API_TOKEN = "SuperSecureTokenValue"

bearer_scheme = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)):
    if credentials.credentials != API_TOKEN:
        logger.warning("Unauthorized access attempt with invalid token")
        raise HTTPException(status_code=401, detail="Invalid or missing API token")
    return True

@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def upload_pdfs(pdf_files: List[UploadFile] = File(...)):
    for pdf_file in pdf_files:
        pdf_path = os.path.join(PDF_STORAGE_DIR, pdf_file.filename)
        with open(pdf_path, "wb") as buffer:
            shutil.copyfileobj(pdf_file.file, buffer)
        pdf_id = os.path.splitext(pdf_file.filename)[0]
        await asyncio.to_thread(transform_and_index, pdf_id, pdf_path)
    return {"status": "success", "message": f"Uploaded and indexed {len(pdf_files)} PDFs"}

@app.post("/search-content", response_class=HTMLResponse)
@limiter.limit("100/minute")
async def search_pdfs(
    request: Request,
    query: str = Form(...),
    exact_match: Optional[str] = Form(None)
):
    exact = exact_match == "on"
    try:
        if exact:
            query_body = {
                "multi_match": {
                    "query": query,
                    "type": "phrase",
                    "fields": ["content", "caption", "table_data", "image_metadata"]
                }
            }
        else:
            query_body = {
                "multi_match": {
                    "query": query,
                    "fields": ["content", "caption", "table_data", "image_metadata"]
                }
            }
        response = es.search(index="pdf_data", query=query_body, size=10)
        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            results.append({
                "doc_id": hit["_id"],
                "type": source.get("type"),
                "page": source.get("page"),
                "content": source.get("content")
            })
    except Exception as e:
        logger.error(f"Search query failed: {e}")
        results = []

    return templates.TemplateResponse("results.html", {
        "request": request,
        "results": results,
        "query": query,
        "exact_match": exact
    })

@app.post("/secure-search-content")  # no response_class=HTMLResponse
@limiter.limit("100/minute")
def search_content(request: Request, request_body: ContentSearchRequest, authorized: bool = Depends(verify_token)):
    try:
        if request_body.exact_match:
            query_body = {
                "multi_match": {
                    "query": request_body.query,
                    "type": "phrase",
                    "fields": ["content", "caption", "table_data", "image_metadata"]
                }
            }
        else:
            query_body = {
                "multi_match": {
                    "query": request_body.query,
                    "fields": ["content", "caption", "table_data", "image_metadata"]
                }
            }
        response = es.search(index="pdf_data", query=query_body, size=request_body.size)
        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            results.append(ContentSearchResult(
                doc_id=hit["_id"],
                type=source.get("type"),
                page=source.get("page"),
                content=source.get("content")
            ))
        return results
    except Exception as e:
        logger.error(f"Search query failed: {e}")
        raise HTTPException(status_code=500, detail="Search query failed")