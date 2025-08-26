# PDF to Elasticsearch ETL Pipeline & Search Web App

This project is a **unified FastAPI application** that extracts paragraphs, tables, and images metadata from PDFs, indexes the data into Elasticsearch with a powerful ngram-based analyzer supporting **partial substring search**, and provides a clean web UI to upload PDFs and search their content dynamically.

---

## Features

- Extract text paragraphs, tables, and image metadata from PDFs using `pdfplumber`.
- Clean, transform, and serialize extracted content as JSON-friendly documents.
- Index documents into Elasticsearch with:
  - Secure SSL connection.
  - Unique deterministic document IDs based on PDF filename, content type, page, and position.
  - Ngram analyzer on text fields (`content`, `caption`, `table_data`) enabling **partial and substring search** ranging from 3 to 10 characters.
- Web UI built with FastAPI and Jinja2 templates:
  - Upload one or multiple PDFs in-browser.
  - See a smooth **upload progress popup** when large files are uploading.
  - Search all uploaded PDFs with options for partial match or exact phrase match.
  - View search results with page/type/content snippets.
- Rate limiting on search API endpoints to prevent abuse.
- Token-based API authentication supported (optional for API endpoints).
- Incremental reindexing by updating existing documents.

---

## Requirements

- Python 3.8 or higher.
- Elasticsearch 9.x running locally or remotely with SSL enabled.
- Python dependencies installed with: pip install -r requirements.txt


---

## Setup & Running Instructions

1. **Prepare Elasticsearch:**

   - Ensure Elasticsearch 9.x is running and accessible.
   - Provide SSL certificates (e.g., `cert_file.pem`) or configure client trust settings as appropriate.

2. **Create/Recreate Elasticsearch Index with Ngram Mapping:**

   - The FastAPI app will automatically delete and recreate the Elasticsearch index `pdf_data` when starting, with ngram analyzer enabled.
   - **Warning:** This will delete existing indexed data; reindex your PDFs afterward.

3. **Run the FastAPI App:** 
   - uvicorn app:app --host 0.0.0.0 --port 8000 --reload
   - Open http://localhost:8000/

Now Use the interface to:

- **Upload PDFs:** Select and upload one or multiple PDFs.  
  A modal popup will show "Upload in progress" until indexing completes.

- **Search PDFs:** Enter text queries to search across **all uploaded PDFs**.  
  Use the "Exact Phrase Match" checkbox to toggle phrase search or partial substring search.

**Search API:**

   - The `/search-content` endpoint accepts unsecured form requests for web UI searches.
   - A secured token-based API is available at `/secure-search-content` for programmatic access.
   - Rate limiting applies: 100 requests per IP per minute.
---

## Additional Notes

- **Partial Search:** Enabled by Elasticsearch's ngram analyzer, allowing efficient substring matching starting from three characters.
- **Upload Progress:** The user interface shows a full-page modal overlay indicating the upload is in progress.
- **Indexing Details:** Tables are indexed by concatenating all cell values to a searchable text string.
- **Performance:** Ngram indexing increases index size but significantly enhances partial match capabilities.
- **Security:** Protect API access with tokens when needed, especially for external clients.

---

## Troubleshooting & Tips

- If partial substring searches yield no results:
  - Confirm Elasticsearch index mapping includes the ngram analyzer.
  - Ensure PDFs have been uploaded and indexed after index creation.
  - Verify search queries target the appropriate analyzed fields.

- To manually delete the Elasticsearch index, use:
   curl -X DELETE "http://localhost:9200/pdf_data"
