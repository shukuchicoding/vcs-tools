import json
import sys
import smtplib
import requests
import argparse
import re
from urllib.parse import unquote
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

_TITLE_PATTERN = re.compile(
    r"(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})\s*-\s*Ca\s*(?P<shift>\d+)",
    re.IGNORECASE,
)

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

def parse_shift_and_date(report_title: str) -> tuple[str, str]:
    """
    Input ví dụ: "18 June 2026 - Ca 2 (19h00 - 07h00) - NOC_Compose - VCS Confluence"
    Output: ("2", "18/06/2026")
    """
    match = _TITLE_PATTERN.search(report_title)
    if not match:
        raise ValueError(f"Không parse được ca/ngày từ title: {report_title!r}")

    day = int(match.group("day"))
    month_name = match.group("month").lower()
    year = int(match.group("year"))
    shift = match.group("shift")

    month = _MONTH_MAP.get(month_name)
    if month is None:
        raise ValueError(f"Không nhận diện được tên tháng: {match.group('month')!r}")

    formatted_date = f"{day:02d}/{month:02d}/{year}"
    return shift, formatted_date

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
# Windows không cho phép các ký tự này trong tên file
_INVALID_FS_CHARS = re.compile(r'[<>:"/\\|?*]')

def _parse_content_disposition_filename(cd: str) -> str | None:
    if not cd:
        return None

    # Ưu tiên filename* (RFC 5987, UTF-8 percent-encoded) — xử lý đúng cả tên có unicode
    match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, re.IGNORECASE)
    if match:
        return unquote(match.group(1).strip())

    # Fallback: filename="..."
    match = re.search(r'filename\s*=\s*"([^"]+)"', cd, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # Fallback: filename=... (không có quote)
    match = re.search(r"filename\s*=\s*([^;]+)", cd, re.IGNORECASE)
    if match:
        return match.group(1).strip().strip('"')

    return None


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
    parsed_name = _parse_content_disposition_filename(cd) if cd else None
    if parsed_name:
        # Sanitize phòng trường hợp server trả thêm ký tự không hợp lệ với filesystem
        filename = _INVALID_FS_CHARS.sub("_", parsed_name)

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

    x, y = parse_shift_and_date(report_title)

    print(f"Downloaded: {file_path}")

    msg = EmailMessage()
    msg["From"] = "NOC Cloudrity <{CONFIG['mail_from']}>"
    msg["To"] = ", ".join(CONFIG["mail_to"])
    msg["Subject"] = "Báo cáo FO 247 ca {x} ngày {y}"

    body = f"""
    <p>Dear các anh,</p>
    <p>Em gửi báo cáo trực ca {x} ngày {y}.</p>
    Người bàn giao: {prev_staffs}<br>
    Người nhận bàn giao: {curr_staffs}<br>
    <p>
        Biên bản bàn giao:
        <a href="{report_url}">{report_title}</a>
    </p>
    Best regards,<br>
    <i>{sender_fullname}</i><br>
    ---
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