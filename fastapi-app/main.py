from __future__ import annotations

import base64
import json
import os
import re
import socket
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import psycopg2
from psycopg2.extras import RealDictCursor

TOKEN_URL = "https://oauth.fatsecret.com/connect/token"
API_URL = "https://platform.fatsecret.com/rest/server.api"
DB_TABLE_NAME = "fatsecret_foods"

_token_cache: dict[str, Any] = {"access_token": None, "expires_at": 0.0}


def _load_local_env_file() -> None:
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


_load_local_env_file()

app = FastAPI(title="FatSecret Proxy API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise HTTPException(status_code=500, detail=f"Missing environment variable: {name}")


def _resolve_query(raw_query: str) -> tuple[str, str]:
    input_query = raw_query
    normalized_input = (raw_query or "").strip()
    default_query = os.getenv("FATSECRET_DEFAULT_QUERY", "a")
    effective_query = normalized_input if len(normalized_input) >= 2 else default_query
    return input_query, effective_query


def _resolve_crawl_terms(raw_terms: str) -> list[str]:
    terms_text = raw_terms.strip()
    if terms_text:
        parts = [term.strip() for term in terms_text.split(",")]
    else:
        configured = os.getenv("FATSECRET_CRAWL_TERMS", "")
        if configured.strip():
            parts = [term.strip() for term in configured.split(",")]
        else:
            # Single-character search terms return nearly identical buckets.
            # Use two-character prefixes by default to cover more unique foods.
            letters = [chr(code) for code in range(ord("a"), ord("z") + 1)]
            parts = [f"{first}{second}" for first in letters for second in letters]

    seen: set[str] = set()
    terms: list[str] = []
    for term in parts:
        if not term:
            continue
        key = term.lower()
        if key in seen:
            continue
        seen.add(key)
        terms.append(term)
    return terms


def _dedupe_key(item: dict[str, Any]) -> str:
    food_id = str(item.get("food_id") or "").strip()
    if food_id:
        return f"id:{food_id}"
    return (
        f"name:{str(item.get('food_name') or '').strip().lower()}|"
        f"brand:{str(item.get('brand_name') or '').strip().lower()}|"
        f"url:{str(item.get('food_url') or '').strip().lower()}"
    )


def _db_connection():
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise HTTPException(status_code=500, detail="Missing DATABASE_URL in environment")
    try:
        return psycopg2.connect(database_url, connect_timeout=5)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {exc}") from exc


def _ensure_food_table(connection) -> None:
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME} (
      id BIGSERIAL PRIMARY KEY,
      food_id TEXT NOT NULL UNIQUE,
      food_name TEXT NOT NULL,
      brand_name TEXT NULL,
      category TEXT NULL,
      calories_per_serving TEXT NULL,
      fat_g TEXT NULL,
      carbs_g TEXT NULL,
      protein_g TEXT NULL,
      macros_g TEXT NULL,
      food_type TEXT NULL,
      food_description TEXT NULL,
      food_url TEXT NULL,
      status TEXT NOT NULL DEFAULT 'Fetched',
      source TEXT NOT NULL DEFAULT 'FatSecret',
      raw_json JSONB NULL,
      last_query TEXT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """
    with connection.cursor() as cursor:
        cursor.execute(create_sql)


def _ensure_food_table_exists() -> None:
    connection = _db_connection()
    try:
        _ensure_food_table(connection)
        connection.commit()
    finally:
        connection.close()


def _truncate_food_table() -> None:
    connection = _db_connection()
    try:
        _ensure_food_table(connection)
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {DB_TABLE_NAME} RESTART IDENTITY")
        connection.commit()
    finally:
        connection.close()


def _count_food_rows() -> int:
    connection = _db_connection()
    try:
        _ensure_food_table(connection)
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {DB_TABLE_NAME}")
            row = cursor.fetchone()
            return int(row[0]) if row else 0
    finally:
        connection.close()


def _upsert_food_rows(rows: list[dict[str, Any]], query_text: str) -> int:
    if not rows:
        return 0

    sql = f"""
    INSERT INTO {DB_TABLE_NAME}
      (food_id, food_name, brand_name, category, calories_per_serving, fat_g, carbs_g, protein_g,
       macros_g, food_type, food_description, food_url, status, source, raw_json, last_query, updated_at)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
    ON CONFLICT (food_id) DO UPDATE SET
      food_name = EXCLUDED.food_name,
      brand_name = EXCLUDED.brand_name,
      category = EXCLUDED.category,
      calories_per_serving = EXCLUDED.calories_per_serving,
      fat_g = EXCLUDED.fat_g,
      carbs_g = EXCLUDED.carbs_g,
      protein_g = EXCLUDED.protein_g,
      macros_g = EXCLUDED.macros_g,
      food_type = EXCLUDED.food_type,
      food_description = EXCLUDED.food_description,
      food_url = EXCLUDED.food_url,
      status = EXCLUDED.status,
      source = EXCLUDED.source,
      raw_json = EXCLUDED.raw_json,
      last_query = EXCLUDED.last_query,
      updated_at = NOW()
    """

    connection = _db_connection()
    try:
        _ensure_food_table(connection)
        saved = 0
        with connection.cursor() as cursor:
            for row in rows:
                food_id = str(row.get("food_id") or "").strip()
                if not food_id:
                    continue
                cursor.execute(
                    sql,
                    (
                        food_id,
                        str(row.get("food_name") or ""),
                        str(row.get("brand_name") or ""),
                        str(row.get("category") or ""),
                        str(row.get("calories_per_serving") or ""),
                        str(row.get("fat_g") or ""),
                        str(row.get("carbs_g") or ""),
                        str(row.get("protein_g") or ""),
                        str(row.get("macros_g") or ""),
                        str(row.get("food_type") or ""),
                        str(row.get("food_description") or ""),
                        str(row.get("food_url") or ""),
                        str(row.get("status") or "Fetched"),
                        str(row.get("source") or "FatSecret"),
                        json.dumps(row.get("raw") or {}, ensure_ascii=False),
                        query_text,
                    ),
                )
                saved += 1
        connection.commit()
        return saved
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _search_saved_foods(q: str, page: int, page_size: int) -> tuple[list[dict[str, Any]], int]:
    query_text = (q or "").strip()
    like_value = f"%{query_text}%"
    offset = (page - 1) * page_size

    count_sql = f"""
    SELECT COUNT(*)
    FROM {DB_TABLE_NAME}
    WHERE %s = ''
       OR food_name ILIKE %s
       OR COALESCE(brand_name, '') ILIKE %s
       OR COALESCE(category, '') ILIKE %s
       OR COALESCE(last_query, '') ILIKE %s
    """

    data_sql = f"""
    SELECT
      id, food_id, food_name, brand_name, category, calories_per_serving,
      fat_g, carbs_g, protein_g, macros_g, food_type, food_description,
      food_url, status, source, raw_json, last_query, created_at, updated_at
    FROM {DB_TABLE_NAME}
    WHERE %s = ''
       OR food_name ILIKE %s
       OR COALESCE(brand_name, '') ILIKE %s
       OR COALESCE(category, '') ILIKE %s
       OR COALESCE(last_query, '') ILIKE %s
    ORDER BY LOWER(food_name) ASC, id ASC
    LIMIT %s OFFSET %s
    """

    connection = _db_connection()
    try:
        _ensure_food_table(connection)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(count_sql, (query_text, like_value, like_value, like_value, like_value))
            total_count = int(cursor.fetchone()["count"])
            cursor.execute(data_sql, (query_text, like_value, like_value, like_value, like_value, page_size, offset))
            rows = cursor.fetchall()
            return rows, total_count
    finally:
        connection.close()


def _get_access_token() -> str:
    # Reuse the token until just before expiry to avoid unnecessary OAuth calls.
    if _token_cache["access_token"] and time.time() < float(_token_cache["expires_at"]) - 30:
        return str(_token_cache["access_token"])

    client_id = _required_env("CLIENT_ID_FATSECRET")
    client_secret = _required_env("CLIENT_SECRET_FATSECRET")
    basic_token = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")

    body = urlencode({"grant_type": "client_credentials", "scope": "basic"}).encode("utf-8")
    request = Request(
        TOKEN_URL,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Basic {basic_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    try:
        with urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="ignore")
        raise HTTPException(
            status_code=502,
            detail=f"FatSecret token request failed: {error_body or exc.reason}",
        ) from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=f"FatSecret token network error: {exc}") from exc

    access_token = payload.get("access_token")
    if not access_token:
        raise HTTPException(status_code=502, detail="FatSecret token response did not include access_token")

    expires_in = int(payload.get("expires_in", 3600))
    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = time.time() + expires_in
    return str(access_token)


def _extract_value(pattern: str, text: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


def _normalize_food(food: dict[str, Any]) -> dict[str, Any]:
    description = str(food.get("food_description") or "")
    calories = _extract_value(r"Calories:\s*([0-9.]+\s*k?cal)", description)
    fat = _extract_value(r"Fat:\s*([0-9.]+g)", description)
    carbs = _extract_value(r"Carbs:\s*([0-9.]+g)", description)
    protein = _extract_value(r"Protein:\s*([0-9.]+g)", description)

    macros = []
    if fat:
        macros.append(f"Fat {fat}")
    if carbs:
        macros.append(f"Carbs {carbs}")
    if protein:
        macros.append(f"Protein {protein}")

    return {
        "food_id": str(food.get("food_id") or ""),
        "food_name": str(food.get("food_name") or ""),
        "brand_name": str(food.get("brand_name") or ""),
        "category": str(food.get("food_type") or ""),
        "calories_per_serving": calories,
        "fat_g": fat.replace("g", "").strip() if fat else "",
        "carbs_g": carbs.replace("g", "").strip() if carbs else "",
        "protein_g": protein.replace("g", "").strip() if protein else "",
        "macros_g": " | ".join(macros),
        "food_type": str(food.get("food_type") or ""),
        "food_description": description,
        "food_url": str(food.get("food_url") or ""),
        "status": "Fetched",
        "source": "FatSecret",
        "raw": food,
    }


def _request_food_page(token: str, q: str, page: int, max_results: int) -> tuple[list[dict[str, Any]], int, int]:
    params = {
        "method": "foods.search",
        "search_expression": q,
        "page_number": page,
        "max_results": max_results,
        "format": "json",
    }
    url = f"{API_URL}?{urlencode(params)}"
    request = Request(url, method="GET", headers={"Authorization": f"Bearer {token}"})

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urlopen(request, timeout=60) as response:
                raw_body = response.read()
                payload = json.loads(raw_body.decode("utf-8"))
            break
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="ignore")
            raise HTTPException(
                status_code=502,
                detail=f"FatSecret search request failed: {error_body or exc.reason}",
            ) from exc
        except (URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.6 * (attempt + 1))
                continue
            raise HTTPException(status_code=502, detail=f"FatSecret search network error: {exc}") from exc
    if last_error and "payload" not in locals():
        raise HTTPException(status_code=502, detail=f"FatSecret search network error: {last_error}")

    foods_obj = payload.get("foods", {})
    items = foods_obj.get("food", [])
    if isinstance(items, dict):
        items = [items]

    total_results_raw = foods_obj.get("total_results", 0)
    try:
        total_results = int(total_results_raw)
    except (TypeError, ValueError):
        total_results = 0
    return items, total_results, len(raw_body)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "FatSecret FastAPI proxy is running"}


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/fatsecret/search")
def search_foods(
    q: str = Query("", description="Optional search text"),
    page: int = Query(0, ge=0, description="Results page number starting from 0"),
    max_results: int = Query(50, ge=1, le=50, description="Max results per request"),
    fetch_all: bool = Query(True, description="If true, fetch all pages"),
    max_total: int = Query(35000, ge=1, le=100000, description="Safety cap for total rows"),
    crawl_all: bool = Query(False, description="If true, crawl multiple query terms and dedupe results"),
    crawl_terms: str = Query("", description="Optional comma-separated crawl terms"),
    crawl_pages_per_term: int = Query(3, ge=1, le=20, description="Pages to fetch for each crawl term"),
    save_to_db: bool = Query(False, description="If true, save fetched data to database"),
    reset_db: bool = Query(False, description="If true and save_to_db=true, clear table before saving"),
) -> dict[str, Any]:
    input_query, effective_query = _resolve_query(q)

    token = _get_access_token()
    all_items: list[dict[str, Any]] = []
    total_results: int | None = 0
    downloaded_bytes = 0
    scanned_count = 0

    if crawl_all:
        unique_items: dict[str, dict[str, Any]] = {}
        terms = _resolve_crawl_terms(crawl_terms)
        max_pages_per_term = crawl_pages_per_term if fetch_all else 1

        for term in terms:
            term_seen = 0
            for current_page in range(max_pages_per_term):
                items, page_total, page_bytes = _request_food_page(
                    token=token,
                    q=term,
                    page=current_page,
                    max_results=max_results,
                )
                downloaded_bytes += page_bytes
                if not items:
                    break

                scanned_count += len(items)
                term_seen += len(items)
                for item in items:
                    key = _dedupe_key(item)
                    if key not in unique_items:
                        unique_items[key] = item
                        if len(unique_items) >= max_total:
                            break
                if len(unique_items) >= max_total:
                    break

                if len(items) < max_results:
                    break
                if page_total and term_seen >= page_total:
                    break

            if len(unique_items) >= max_total:
                break

        all_items = list(unique_items.values())
        total_results = None
    else:
        if fetch_all:
            current_page = page
            while len(all_items) < max_total:
                items, page_total, page_bytes = _request_food_page(
                    token=token,
                    q=effective_query,
                    page=current_page,
                    max_results=max_results,
                )
                downloaded_bytes += page_bytes
                if page_total:
                    total_results = page_total
                if not items:
                    break

                all_items.extend(items)
                scanned_count += len(items)
                if len(items) < max_results:
                    break
                if total_results and len(all_items) >= total_results:
                    break

                current_page += 1
                if current_page - page > 2000:
                    break
        else:
            items, page_total, page_bytes = _request_food_page(
                token=token,
                q=effective_query,
                page=page,
                max_results=max_results,
            )
            downloaded_bytes += page_bytes
            total_results = page_total
            all_items.extend(items)
            scanned_count += len(items)

    if len(all_items) > max_total:
        all_items = all_items[:max_total]

    if crawl_all:
        progress_percent = 100.0
    else:
        safe_total = min(total_results, max_total) if total_results else len(all_items)
        progress_percent = round((len(all_items) / safe_total) * 100, 2) if safe_total else 0.0

    foods = [_normalize_food(item) for item in all_items]
    saved_count = 0
    db_error = None
    db_total_count = None
    fatsecret_total_reported: int | None = total_results if not crawl_all else None
    if save_to_db:
        try:
            if reset_db:
                _truncate_food_table()
            else:
                _ensure_food_table_exists()
            saved_count = _upsert_food_rows(foods, query_text=effective_query)
            db_total_count = _count_food_rows()
        except Exception as exc:
            db_error = str(exc)
    else:
        try:
            db_total_count = _count_food_rows()
        except Exception:
            db_total_count = None

    return {
        "mode": "crawl_all" if crawl_all else "query_search",
        "query": effective_query,
        "input_query": input_query,
        "page": page,
        "max_results": max_results,
        "fetch_all": fetch_all,
        "max_total": max_total,
        "crawl_all": crawl_all,
        "crawl_pages_per_term": crawl_pages_per_term,
        "crawl_terms_count": len(_resolve_crawl_terms(crawl_terms)) if crawl_all else 0,
        "total_results": total_results,
        "fatsecret_total_reported": fatsecret_total_reported,
        "fetched_count": len(foods),
        "scanned_count": scanned_count,
        "progress_percent": progress_percent,
        "downloaded_bytes": downloaded_bytes,
        "downloaded_mb": round(downloaded_bytes / (1024 * 1024), 2),
        "save_to_db": save_to_db,
        "reset_db": reset_db,
        "saved_count": saved_count,
        "db_total_count": db_total_count,
        "db_table": DB_TABLE_NAME,
        "db_error": db_error,
        "foods": foods,
    }


@app.get("/api/fatsecret/saved")
def search_saved_foods(
    q: str = Query("", description="Search text on saved DB records"),
    page: int = Query(1, ge=1, description="Page number starting from 1"),
    page_size: int = Query(100, ge=1, le=500, description="Rows per page"),
) -> dict[str, Any]:
    rows, total_count = _search_saved_foods(q=q, page=page, page_size=page_size)
    total_pages = (total_count + page_size - 1) // page_size if page_size else 1
    return {
        "query": q,
        "page": page,
        "page_size": page_size,
        "total_count": total_count,
        "total_pages": total_pages,
        "count": len(rows),
        "db_table": DB_TABLE_NAME,
        "foods": rows,
    }


@app.get("/api/fatsecret/search/stream")
def search_foods_stream(
    q: str = Query("", description="Optional search text"),
    page: int = Query(0, ge=0, description="Results page number starting from 0"),
    max_results: int = Query(50, ge=1, le=50, description="Max results per request"),
    fetch_all: bool = Query(True, description="If true, fetch all pages"),
    max_total: int = Query(35000, ge=1, le=100000, description="Safety cap for total rows"),
    crawl_all: bool = Query(False, description="If true, crawl multiple query terms and dedupe results"),
    crawl_terms: str = Query("", description="Optional comma-separated crawl terms"),
    crawl_pages_per_term: int = Query(3, ge=1, le=20, description="Pages to fetch for each crawl term"),
    save_to_db: bool = Query(True, description="If true, save fetched data to database"),
    reset_db: bool = Query(False, description="If true and save_to_db=true, clear table before saving"),
) -> StreamingResponse:
    input_query, effective_query = _resolve_query(q)

    def _line(payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False) + "\n"

    def event_stream():
        all_items: list[dict[str, Any]] = []
        total_results: int | None = 0
        downloaded_bytes = 0
        pre_db_error = None
        saved_count = 0
        scanned_count = 0
        fatsecret_total_reported: int | None = None
        term_reported_totals: dict[str, int] = {}

        try:
            if save_to_db:
                try:
                    if reset_db:
                        _truncate_food_table()
                    else:
                        _ensure_food_table_exists()
                except Exception as exc:
                    pre_db_error = str(exc)

            token = _get_access_token()

            if crawl_all:
                unique_items: dict[str, dict[str, Any]] = {}
                terms = _resolve_crawl_terms(crawl_terms)
                max_pages_per_term = crawl_pages_per_term if fetch_all else 1

                for term_index, term in enumerate(terms):
                    term_seen = 0
                    for current_page in range(max_pages_per_term):
                        items, page_total, page_bytes = _request_food_page(
                            token=token,
                            q=term,
                            page=current_page,
                            max_results=max_results,
                        )
                        downloaded_bytes += page_bytes
                        if not items:
                            break

                        scanned_count += len(items)
                        term_seen += len(items)
                        if page_total:
                            previous_total = term_reported_totals.get(term, 0)
                            if page_total > previous_total:
                                term_reported_totals[term] = page_total
                                fatsecret_total_reported = sum(term_reported_totals.values())
                        new_unique_items: list[dict[str, Any]] = []
                        for item in items:
                            key = _dedupe_key(item)
                            if key not in unique_items:
                                unique_items[key] = item
                                new_unique_items.append(item)
                                if len(unique_items) >= max_total:
                                    break

                        if save_to_db and not pre_db_error and new_unique_items:
                            try:
                                normalized_batch = [_normalize_food(raw_item) for raw_item in new_unique_items]
                                saved_count += _upsert_food_rows(normalized_batch, query_text=term)
                            except Exception as exc:
                                pre_db_error = str(exc)

                        all_items = list(unique_items.values())
                        progress_percent = round((len(all_items) / max_total) * 100, 2) if max_total else 0.0
                        yield _line(
                            {
                                "type": "progress",
                                "mode": "crawl_all",
                                "query": effective_query,
                                "term": term,
                                "term_index": term_index + 1,
                                "terms_total": len(terms),
                                "page": current_page,
                                "fetched_count": len(all_items),
                                "scanned_count": scanned_count,
                                "total_results": None,
                                "fatsecret_total_reported": fatsecret_total_reported,
                                "progress_percent": progress_percent,
                                "downloaded_bytes": downloaded_bytes,
                                "downloaded_mb": round(downloaded_bytes / (1024 * 1024), 2),
                                "saved_count": saved_count,
                                "db_error": pre_db_error,
                            }
                        )

                        if len(unique_items) >= max_total:
                            break
                        if len(items) < max_results:
                            break
                        if page_total and term_seen >= page_total:
                            break

                    if len(unique_items) >= max_total:
                        break
                total_results = None
            else:
                if fetch_all:
                    current_page = page
                    while len(all_items) < max_total:
                        items, page_total, page_bytes = _request_food_page(
                            token=token,
                            q=effective_query,
                            page=current_page,
                            max_results=max_results,
                        )
                        downloaded_bytes += page_bytes

                        if page_total:
                            total_results = page_total
                            fatsecret_total_reported = total_results
                        if items:
                            all_items.extend(items)
                            scanned_count += len(items)
                            if save_to_db and not pre_db_error:
                                try:
                                    normalized_batch = [_normalize_food(raw_item) for raw_item in items]
                                    saved_count += _upsert_food_rows(normalized_batch, query_text=effective_query)
                                except Exception as exc:
                                    pre_db_error = str(exc)

                        safe_total = min(total_results, max_total) if total_results else max_total
                        progress_percent = round((min(len(all_items), safe_total) / safe_total) * 100, 2) if safe_total else 0.0
                        yield _line(
                            {
                                "type": "progress",
                                "mode": "query_search",
                                "query": effective_query,
                                "page": current_page,
                                "fetched_count": len(all_items),
                                "scanned_count": scanned_count,
                                "total_results": total_results,
                                "fatsecret_total_reported": fatsecret_total_reported,
                                "progress_percent": progress_percent,
                                "downloaded_bytes": downloaded_bytes,
                                "downloaded_mb": round(downloaded_bytes / (1024 * 1024), 2),
                                "saved_count": saved_count,
                                "db_error": pre_db_error,
                            }
                        )

                        if not items:
                            break
                        if len(items) < max_results:
                            break
                        if total_results and len(all_items) >= total_results:
                            break

                        current_page += 1
                        if current_page - page > 2000:
                            break
                else:
                    items, total_results, page_bytes = _request_food_page(
                        token=token,
                        q=effective_query,
                        page=page,
                        max_results=max_results,
                    )
                    downloaded_bytes += page_bytes
                    all_items.extend(items)
                    scanned_count += len(items)
                    fatsecret_total_reported = total_results
                    if save_to_db and not pre_db_error and items:
                        try:
                            normalized_batch = [_normalize_food(raw_item) for raw_item in items]
                            saved_count += _upsert_food_rows(normalized_batch, query_text=effective_query)
                        except Exception as exc:
                            pre_db_error = str(exc)
                    safe_total = min(total_results, max_total) if total_results else len(all_items)
                    progress_percent = round((len(all_items) / safe_total) * 100, 2) if safe_total else 0.0
                    yield _line(
                        {
                            "type": "progress",
                            "mode": "query_search",
                            "query": effective_query,
                            "page": page,
                            "fetched_count": len(all_items),
                            "scanned_count": scanned_count,
                            "total_results": total_results,
                            "fatsecret_total_reported": fatsecret_total_reported,
                            "progress_percent": progress_percent,
                            "downloaded_bytes": downloaded_bytes,
                            "downloaded_mb": round(downloaded_bytes / (1024 * 1024), 2),
                            "saved_count": saved_count,
                            "db_error": pre_db_error,
                        }
                    )

            if len(all_items) > max_total:
                all_items = all_items[:max_total]

            if crawl_all:
                progress_percent = 100.0
            else:
                safe_total = min(total_results, max_total) if total_results else len(all_items)
                progress_percent = round((len(all_items) / safe_total) * 100, 2) if safe_total else 0.0
            foods = [_normalize_food(item) for item in all_items]
            db_error = pre_db_error
            db_total_count = None
            if save_to_db and not db_error:
                try:
                    db_total_count = _count_food_rows()
                except Exception as exc:
                    db_error = str(exc)
            else:
                try:
                    db_total_count = _count_food_rows()
                except Exception:
                    db_total_count = None

            yield _line(
                {
                    "type": "complete",
                    "mode": "crawl_all" if crawl_all else "query_search",
                    "query": effective_query,
                    "input_query": input_query,
                    "page": page,
                    "max_results": max_results,
                    "fetch_all": fetch_all,
                    "max_total": max_total,
                    "crawl_all": crawl_all,
                    "crawl_pages_per_term": crawl_pages_per_term,
                    "crawl_terms_count": len(_resolve_crawl_terms(crawl_terms)) if crawl_all else 0,
                    "total_results": total_results,
                    "fatsecret_total_reported": fatsecret_total_reported,
                    "fetched_count": len(foods),
                    "scanned_count": scanned_count,
                    "progress_percent": progress_percent,
                    "downloaded_bytes": downloaded_bytes,
                    "downloaded_mb": round(downloaded_bytes / (1024 * 1024), 2),
                    "save_to_db": save_to_db,
                    "reset_db": reset_db,
                    "saved_count": saved_count,
                    "db_total_count": db_total_count,
                    "db_table": DB_TABLE_NAME,
                    "db_error": db_error,
                    "foods": foods,
                }
            )
        except HTTPException as exc:
            yield _line({"type": "error", "detail": str(exc.detail)})
        except Exception as exc:
            yield _line({"type": "error", "detail": str(exc)})

    return StreamingResponse(event_stream(), media_type="application/x-ndjson")
