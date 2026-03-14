from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from scraper import run_serper_scan, append_to_google_sheet

app = FastAPI()

# React ke liye CORS permission
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/scan")
def scan(keyword: str, location: str, radius: int, filter: str = "all", search_type: str = "places", api_key: str = None):
    return StreamingResponse(
        run_serper_scan(keyword, location, radius, filter, search_type, api_key), 
        media_type="text/event-stream"
    )

@app.post("/save")
async def save_to_sheets(request: Request):
    data = await request.json()
    leads = data.get("leads", [])
    sheet_url = data.get("sheetUrl")
    sheet_name = data.get("sheetName")
    
    success = append_to_google_sheet(leads, sheet_url, sheet_name)
    if success:
        return {"status": "success", "message": "Pipeline sync complete."}
    else:
        return {"status": "error", "message": "Google Sheets pipeline failed. Check configuration."}