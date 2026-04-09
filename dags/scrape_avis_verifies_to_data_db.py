import html
import json
import re
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime


def _extract_first(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text, flags=re.DOTALL)
    if not match:
        return None
    return html.unescape(match.group(1)).strip()


def _parse_reviews(page_html: str) -> list[dict]:
    starts = [m.start() for m in re.finditer(r'<li id="reviews__item__\d+" class="skp-review-item"', page_html)]
    if not starts:
        return []

    blocks: list[str] = []
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if idx + 1 < len(starts) else len(page_html)
        blocks.append(page_html[start:end])

    reviews = []
    for block in blocks:
        review_uid = _extract_first(r'data-global-modal-review-id="([^"]+)"', block)
        author = _extract_first(r'<p class="skp-review-item__info">\s*(.*?)\s*</p>', block)
        rating_raw = _extract_first(r'data-rating="(\d+)"', block)
        review_date = _extract_first(r'<p class="skp-review-item__date">\s*(.*?)\s*</p>', block)
        review_text = _extract_first(r'<p class="skp-review-item__text[^"]*">\s*(.*?)\s*</p>', block)
        experience_date = _extract_first(r'<p class="skp-review-item__summary">\s*Expérience du:\s*(.*?)\s*</p>', block)
        moderation_blocks = re.findall(
            r'<div class="skp-review-moderation[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
            block,
            flags=re.DOTALL,
        )
        responses = []
        for response_rank, moderation_block in enumerate(moderation_blocks, start=1):
            response_header = _extract_first(
                r'<p class="skp-review-moderation__header">\s*(.*?)\s*</p>',
                moderation_block,
            )
            response_text = _extract_first(
                r'<div class="skp-review-moderation__text">\s*(.*?)\s*</div>',
                moderation_block,
            )
            if not response_header and not response_text:
                continue
            responses.append(
                {
                    "response_rank": response_rank,
                    "response_header_raw": response_header,
                    "response_text_raw": response_text,
                }
            )

        if not review_uid:
            continue

        reviews.append(
            {
                "review_uid": review_uid,
                "author": author,
                "rating": int(rating_raw) if rating_raw else None,
                "review_date_raw": review_date,
                "experience_date_raw": experience_date,
                "review_text": review_text,
                "source": "avis-verifies",
                "responses": responses,
            }
        )

    return reviews


def _extract_next_page_url(page_html: str, current_url: str) -> str | None:
    match = re.search(
        r'<a[^>]*class="[^"]*skp-pagination__arrow[^"]*skp-pagination__arrow--next[^"]*"[^>]*href="([^"]+)"',
        page_html,
        flags=re.DOTALL,
    )
    if not match:
        return None
    return urljoin(current_url, html.unescape(match.group(1)).strip())


def _download_page(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            )
        },
    )
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


@dag(
    dag_id="scrape_avis_verifies_to_data_db",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["scraping", "postgres", "rfm"],
)
def scrape_avis_verifies_to_data_db():
    @task()
    def extract_reviews() -> dict:
        start_url = "https://www.avis-verifies.com/avis-clients/esthima.fr"
        resume_from_url = Variable.get("AVIS_VERIFIES_RESUME_URL", default_var=start_url)
        next_cursor_url = Variable.get("AVIS_VERIFIES_NEXT_URL", default_var="").strip()
        # V2 cursor mode: continue from NEXT_URL when available, else fallback.
        if next_cursor_url:
            current_url = next_cursor_url
            cursor_mode = "next_url"
        else:
            current_url = resume_from_url
            cursor_mode = "resume_url"
        initial_url = current_url
        max_pages = int(Variable.get("AVIS_VERIFIES_MAX_PAGES", default_var="200"))
        visited_urls = set()
        all_reviews: list[dict] = []
        last_page_url = None
        next_url = None

        page_count = 0

        while current_url and current_url not in visited_urls and page_count < max_pages:
            visited_urls.add(current_url)
            page_count += 1
            last_page_url = current_url
            page_html = _download_page(current_url)
            page_reviews = _parse_reviews(page_html)
            all_reviews.extend(page_reviews)
            print(f"Page {page_count}: {len(page_reviews)} avis extraits ({current_url})")

            next_url = _extract_next_page_url(page_html, current_url)
            current_url = next_url

        print(f"Nombre total d'avis extraits (brut): {len(all_reviews)}")
        return {
            "reviews": all_reviews,
            "page_count": page_count,
            "started_from_url": initial_url,
            "cursor_mode": cursor_mode,
            "last_page_url": last_page_url,
            "next_url": next_url,
        }

    @task()
    def transform_reviews(extracted: dict) -> dict:
        reviews = extracted["reviews"]
        deduped_by_uid = {}
        for review in reviews:
            review_text = (review.get("review_text") or "").strip()
            author = (review.get("author") or "").strip()
            review["review_text"] = re.sub(r"\s+", " ", review_text)
            review["author"] = re.sub(r"\s+", " ", author)
            deduped_by_uid[review["review_uid"]] = review

        transformed_reviews = list(deduped_by_uid.values())
        print(f"Nombre d'avis apres transform (uniques): {len(transformed_reviews)}")
        return {
            "reviews": transformed_reviews,
            "page_count": extracted["page_count"],
            "started_from_url": extracted["started_from_url"],
            "cursor_mode": extracted["cursor_mode"],
            "last_page_url": extracted["last_page_url"],
            "next_url": extracted["next_url"],
        }

    @task()
    def load_to_postgres(payload: dict) -> dict:
        reviews = payload["reviews"]
        if not reviews:
            print("Aucun avis a inserer.")
            return {"loaded_reviews": 0, "loaded_responses": 0}

        hook = PostgresHook(postgres_conn_id="DATA-DB")
        conn = hook.get_conn()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.avis_verifies_reviews (
            review_uid TEXT PRIMARY KEY,
            author TEXT,
            rating INTEGER,
            review_date_raw TEXT,
            experience_date_raw TEXT,
            review_text TEXT,
            source TEXT NOT NULL DEFAULT 'avis-verifies',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        create_responses_table_sql = """
        CREATE TABLE IF NOT EXISTS public.avis_verifies_review_responses (
            review_uid TEXT NOT NULL,
            response_rank INTEGER NOT NULL,
            response_header_raw TEXT,
            response_text_raw TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (review_uid, response_rank),
            FOREIGN KEY (review_uid) REFERENCES public.avis_verifies_reviews (review_uid) ON DELETE CASCADE
        );
        """

        upsert_sql = """
        INSERT INTO public.avis_verifies_reviews (
            review_uid,
            author,
            rating,
            review_date_raw,
            experience_date_raw,
            review_text,
            source,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (review_uid) DO UPDATE
        SET
            author = EXCLUDED.author,
            rating = EXCLUDED.rating,
            review_date_raw = EXCLUDED.review_date_raw,
            experience_date_raw = EXCLUDED.experience_date_raw,
            review_text = EXCLUDED.review_text,
            source = EXCLUDED.source,
            updated_at = NOW();
        """
        upsert_response_sql = """
        INSERT INTO public.avis_verifies_review_responses (
            review_uid,
            response_rank,
            response_header_raw,
            response_text_raw,
            updated_at
        ) VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (review_uid, response_rank) DO UPDATE
        SET
            response_header_raw = EXCLUDED.response_header_raw,
            response_text_raw = EXCLUDED.response_text_raw,
            updated_at = NOW();
        """

        rows = [
            (
                review["review_uid"],
                review["author"],
                review["rating"],
                review["review_date_raw"],
                review["experience_date_raw"],
                review["review_text"],
                review["source"],
            )
            for review in reviews
        ]
        response_rows = []
        for review in reviews:
            for response in review.get("responses", []):
                response_rows.append(
                    (
                        review["review_uid"],
                        response["response_rank"],
                        response["response_header_raw"],
                        response["response_text_raw"],
                    )
                )

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                cursor.execute(create_responses_table_sql)
                cursor.executemany(upsert_sql, rows)
                if response_rows:
                    cursor.executemany(upsert_response_sql, response_rows)

        print(f"Nombre d'avis upsert en base: {len(rows)}")
        print(f"Nombre de reponses upsert en base: {len(response_rows)}")
        return {"loaded_reviews": len(rows), "loaded_responses": len(response_rows)}

    @task()
    def persist_etl_state(extracted: dict, loaded: dict) -> None:
        run_meta = {
            "started_from_url": extracted["started_from_url"],
            "cursor_mode": extracted["cursor_mode"],
            "last_page_url": extracted["last_page_url"],
            "next_url": extracted["next_url"],
            "page_count": extracted["page_count"],
            "loaded_reviews": loaded["loaded_reviews"],
            "loaded_responses": loaded["loaded_responses"],
            "updated_at": pendulum.now("UTC").to_iso8601_string(),
        }
        Variable.set("AVIS_VERIFIES_LAST_RUN_META", json.dumps(run_meta))
        # Keep pagination cursor in a dedicated variable for ETL continuity.
        Variable.set("AVIS_VERIFIES_NEXT_URL", extracted["next_url"] or "")

    extracted = extract_reviews()
    transformed = transform_reviews(extracted)
    loaded = load_to_postgres(transformed)
    persist_etl_state(extracted, loaded)


scrape_avis_verifies_to_data_db()
