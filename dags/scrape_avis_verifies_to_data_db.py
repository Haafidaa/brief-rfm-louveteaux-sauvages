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


def _strip_html_tags(text: str | None) -> str | None:
    if text is None:
        return None
    no_tags = re.sub(r"<[^>]+>", " ", text, flags=re.DOTALL)
    normalized = re.sub(r"\s+", " ", html.unescape(no_tags)).strip()
    return normalized or None


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
        moderation_headers = re.findall(
            r'<p class="skp-review-moderation__header">\s*(.*?)\s*</p>',
            block,
            flags=re.DOTALL,
        )
        moderation_texts = re.findall(
            r'<div class="skp-review-moderation__text">\s*(.*?)\s*</div>',
            block,
            flags=re.DOTALL,
        )
        responses = []
        response_count = max(len(moderation_headers), len(moderation_texts))
        for response_rank in range(response_count):
            response_header = _strip_html_tags(
                moderation_headers[response_rank] if response_rank < len(moderation_headers) else None
            )
            response_text = _strip_html_tags(
                moderation_texts[response_rank] if response_rank < len(moderation_texts) else None
            )
            if not response_header and not response_text:
                continue
            responses.append(
                {
                    "response_rank": response_rank + 1,
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
    # Preferred source: HTML head pagination link.
    rel_next_match = re.search(
        r'<link[^>]*rel="next"[^>]*href="([^"]+)"',
        page_html,
        flags=re.DOTALL,
    )
    if rel_next_match:
        return urljoin(current_url, html.unescape(rel_next_match.group(1)).strip())

    # Fallback source: next arrow in pagination controls.
    match = re.search(
        r'<a[^>]*class="[^"]*skp-pagination__arrow--next[^"]*"[^>]*href="([^"]+)"',
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
        page_urls: list[str] = []
        last_page_url = None
        next_url = None

        page_count = 0

        while current_url and current_url not in visited_urls and page_count < max_pages:
            visited_urls.add(current_url)
            page_urls.append(current_url)
            page_count += 1
            last_page_url = current_url
            page_html = _download_page(current_url)
            print(f"Page {page_count}: pagination detectee ({current_url})")

            next_url = _extract_next_page_url(page_html, current_url)
            current_url = next_url

        print(f"Nombre total de pages a traiter: {len(page_urls)}")
        return {
            "page_urls": page_urls,
            "page_count": page_count,
            "started_from_url": initial_url,
            "cursor_mode": cursor_mode,
            "last_page_url": last_page_url,
            "next_url": next_url,
        }

    @task()
    def load_to_postgres(payload: dict) -> dict:
        page_urls = payload["page_urls"]
        if not page_urls:
            print("Aucune page a traiter.")
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

        loaded_reviews = 0
        loaded_responses = 0
        seen_review_ids: set[str] = set()

        for page_index, page_url in enumerate(page_urls, start=1):
            page_html = _download_page(page_url)
            page_reviews = _parse_reviews(page_html)
            rows = []
            response_rows = []

            for review in page_reviews:
                review_uid = review["review_uid"]
                if review_uid in seen_review_ids:
                    continue
                seen_review_ids.add(review_uid)

                review_text = re.sub(r"\s+", " ", (review.get("review_text") or "").strip())
                author = re.sub(r"\s+", " ", (review.get("author") or "").strip())

                rows.append(
                    (
                        review_uid,
                        author,
                        review["rating"],
                        review["review_date_raw"],
                        review["experience_date_raw"],
                        review_text,
                        review["source"],
                    )
                )

                for response in review.get("responses", []):
                    response_rows.append(
                        (
                            review_uid,
                            response["response_rank"],
                            response["response_header_raw"],
                            response["response_text_raw"],
                        )
                    )

            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    cursor.execute(create_responses_table_sql)
                    if rows:
                        cursor.executemany(upsert_sql, rows)
                    if response_rows:
                        cursor.executemany(upsert_response_sql, response_rows)

            loaded_reviews += len(rows)
            loaded_responses += len(response_rows)
            print(f"Page {page_index}/{len(page_urls)} chargee: {len(rows)} avis, {len(response_rows)} reponses")

        print(f"Nombre total d'avis upsert en base: {loaded_reviews}")
        print(f"Nombre total de reponses upsert en base: {loaded_responses}")
        return {"loaded_reviews": loaded_reviews, "loaded_responses": loaded_responses}

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
    loaded = load_to_postgres(extracted)
    persist_etl_state(extracted, loaded)


scrape_avis_verifies_to_data_db()
