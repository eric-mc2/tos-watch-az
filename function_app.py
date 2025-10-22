import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()

@app.route(route="scraper", auth_level=func.AuthLevel.FUNCTION)
def wayback_scraper(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function to collect wayback snapshots from URLs stored in blob storage"""
    from scraper import main as scraper_main
    return scraper_main(req)

@app.route(route="seed_urls", auth_level=func.AuthLevel.FUNCTION)
def seed_urls(req: func.HttpRequest) -> func.HttpResponse:
    """Post seed URLs to blob storage for scraping"""
    from seed_urls import main as seed_main
    return seed_main(req)
