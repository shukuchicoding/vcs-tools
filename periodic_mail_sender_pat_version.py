import json
import sys
import smtplib
import requests
import argparse

from email.message import EmailMessage
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from datetime import timedelta, timezone


BANGKOK_TZ = timezone(timedelta(hours=7))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

EXPORT_TEMPLATE_ID = "4e9c2265-c90e-456a-a5d7-5c7fe09342e5"
EXPORT_VARIANT_ID = "7F0000010175551BAE4736E652E83540"

DOWNLOAD_DIR = Path("../bao-cao-ca")




# =========================
# CONFIG
# =========================
def load_json_file(file_name: str):
    base_dir = Path(__file__).resolve().parent
    file_path = base_dir / file_name
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


CONFIG = load_json_file("config.json")


# =========================
# UTILS
# =========================
def parse_page_id(report_url: str) -> str:
    parsed = urlparse(report_url)
    query = parse_qs(parsed.query)
    page_ids = query.get("pageId")

    if not page_ids:
        raise ValueError("Missing pageId in URL")

    return page_ids[0]


def create_pat_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {CONFLUENCE_PAT}"
    })
    return session


# =========================
# FETCH HTML
# =========================
def fetch_report_html(session: requests.Session, report_url: str) -> str:
    resp = session.get(report_url, timeout=60)
    resp.raise_for_status()
    return resp.text


# =========================
# PARSE HTML
# =========================
def extract_staffs_from_html(html: str):
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    report_title = title_tag.get_text(strip=True) if title_tag else "Biên bản bàn giao"

    main_div = soup.select_one("div#main-content")
    if not main_div:
        raise ValueError("Missing div#main-content")

    p_nodes = main_div.find_all("p", recursive=False)
    if len(p_nodes) < 2:
        raise ValueError("Not enough <p> tags")

    def get_usernames(p_tag):
        return [
            a.get("data-username")
            for a in p_tag.select("a[data-username]")
            if a.get("data-username")
        ]

    prev_staffs_vec = get_usernames(p_nodes[0])
    curr_staffs_vec = get_usernames(p_nodes[1])

    return report_title, prev_staffs_vec, curr_staffs_vec


def resolve_sender_fullname(curr_staffs_vec: list[str]) -> str:
    sender_map = load_json_file("mail_sender_staffs.json")

    for username in curr_staffs_vec:
        if username in sender_map:
            return sender_map[username]

    raise ValueError("No sender mapping found")


# =========================
# EXPORT SYNC (CORE)
# =========================
def export_sync(session: requests.Session, page_id: str) -> Path:
    base_url = CONFIG["baocaoca_export_api_url"]

    url = base_url + "/public/1/export-sync"

    params = {
        "templateId": EXPORT_TEMPLATE_ID,
        "pageId": page_id,
        "scope": "descendants",
        "variantId": EXPORT_VARIANT_ID,
        "locale": "en-US"
    }

    resp = session.get(url, params=params, timeout=300, stream=True)

    print(f"Export status: {resp.status_code}")
    resp.raise_for_status()

    filename = f"{page_id}.docx"

    cd = resp.headers.get("Content-Disposition")
    if cd and "filename=" in cd:
        filename = cd.split("filename=")[-1].replace('"', '').strip()

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_path = DOWNLOAD_DIR / filename

    with open(file_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    return file_path


# =========================
# EMAIL
# =========================
def send_email(msg: EmailMessage):
    with smtplib.SMTP_SSL(CONFIG["smtp_host"], 465) as server:
        server.login(
            CONFIG["smtp_username"],
            CONFIG["smtp_password"]
        )
        server.send_message(msg)


# =========================
# MAIN FLOW
# =========================
def run(report_url: str):
    page_id = parse_page_id(report_url)

    session = create_pat_session()

    html = fetch_report_html(session, report_url)
    report_title, prev_vec, curr_vec = extract_staffs_from_html(html)

    prev_staffs = ", ".join(prev_vec)
    curr_staffs = ", ".join(curr_vec)

    sender_fullname = resolve_sender_fullname(curr_vec)

    file_path = export_sync(session, page_id)

    print(f"Downloaded: {file_path}")

    msg = EmailMessage()
    msg["From"] = f"{sender_fullname} <{CONFIG['mail_from']}>"
    msg["To"] = ", ".join(CONFIG["mail_to"])
    msg["Subject"] = "Báo cáo bàn giao"

    body = f"""
    <p>Report: {report_title}</p>
    <p>From: {prev_staffs}</p>
    <p>To: {curr_staffs}</p>
    <p>Link: {report_url}</p>
    """

    msg.set_content("HTML email")
    msg.add_alternative(body, subtype="html")

    msg.add_attachment(
        file_path.read_bytes(),
        maintype="application",
        subtype="octet-stream",
        filename=file_path.name
    )

    send_email(msg)

    print("DONE")


# =========================
# CLI ENTRY
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="Confluence page URL from user")

    args = parser.parse_args()

    try:
        run(args.url)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)