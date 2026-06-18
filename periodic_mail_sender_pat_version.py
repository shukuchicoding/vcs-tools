import json
import sys
import smtplib
import requests
import argparse

from email.message import EmailMessage
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone


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
        raise ValueError("Missing pageId trong URL")
    return page_ids[0]


def create_pat_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {CONFLUENCE_PAT}"
    })
    return session


# =========================
# FETCH HTML (giữ nguyên)
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
        raise ValueError("Không tìm thấy div#main-content")

    p_nodes = main_div.find_all("p", recursive=False)
    if len(p_nodes) < 2:
        raise ValueError("Không tìm thấy đủ 2 thẻ <p>")

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

    raise ValueError("Không tìm thấy sender mapping")


# =========================
# 🔥 EXPORT-SYNC (NEW CORE)
# =========================
def export_sync(session: requests.Session, report_url: str, page_id: str) -> Path:
    base_url = CONFIG["baocaoca_export_api_url"]

    params = {
        "templateId": EXPORT_TEMPLATE_ID,
        "pageId": page_id,
        "scope": "descendants",
        "variantId": EXPORT_VARIANT_ID,
        "locale": "en-US"
    }

    resp = session.get(base_url + "/public/1/export-sync", params=params, timeout=300, stream=True)

    print(f"Export status: {resp.status_code}")
    resp.raise_for_status()

    filename = "report.docx"
    content_disposition = resp.headers.get("Content-Disposition")

    if content_disposition and "filename=" in content_disposition:
        filename = content_disposition.split("filename=")[-1].strip().replace('"', '')
    else:
        filename = f"{page_id}.docx"

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
def build_email_message(
    prev_staffs: str,
    curr_staffs: str,
    sender_fullname: str,
    report_title: str,
    report_url: str,
    file_path: Path,
    receivers: list[str],
) -> EmailMessage:

    subject = "Báo cáo bàn giao"

    body_html = f"""
    <p>Dear các anh,</p>

    <p>Em gửi báo cáo bàn giao.</p>

    <p>
        Người bàn giao: {prev_staffs}<br>
        Người nhận: {curr_staffs}<br>
        Report: <a href="{report_url}">{report_title}</a>
    </p>

    <p>
        Regards,<br>
        <b>{sender_fullname}</b>
    </p>
    """

    msg = EmailMessage()
    msg["From"] = f"{sender_fullname} <{CONFIG['mail_from']}>"
    msg["To"] = ", ".join(receivers)
    msg["Subject"] = subject

    msg.set_content("HTML email")
    msg.add_alternative(body_html, subtype="html")

    msg.add_attachment(
        file_path.read_bytes(),
        maintype="application",
        subtype="octet-stream",
        filename=file_path.name
    )

    return msg


def send_email(msg: EmailMessage):
    with smtplib.SMTP_SSL(CONFIG["smtp_host"], 465) as server:
        server.login(
            CONFIG["smtp_username"],
            CONFIG["smtp_password"]
        )
        server.send_message(msg)


# =========================
# MAIN
# =========================
def run(report_url: str):
    page_id = parse_page_id(report_url)

    session = create_pat_session()

    # 1. fetch html (giữ lại để parse staff)
    html = fetch_report_html(session, report_url)
    report_title, prev_vec, curr_vec = extract_staffs_from_html(html)

    prev_staffs = ", ".join(prev_vec)
    curr_staffs = ", ".join(curr_vec)

    sender_fullname = resolve_sender_fullname(curr_vec)

    # 2. EXPORT (NEW SIMPLE FLOW)
    file_path = export_sync(session, report_url, page_id)

    print(f"Downloaded: {file_path}")

    # 3. EMAIL
    msg = build_email_message(
        prev_staffs,
        curr_staffs,
        sender_fullname,
        report_title,
        report_url,
        file_path,
        CONFIG["mail_to"]
    )

    send_email(msg)

    print("DONE")


# =========================
# CLI
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="Confluence page URL")
    args = parser.parse_args()

    try:
        run(args.url)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)