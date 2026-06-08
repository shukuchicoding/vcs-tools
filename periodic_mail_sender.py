import json
import sys
import time
import smtplib
import os
from email.message import EmailMessage
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime, timedelta, timezone


BANGKOK_TZ = timezone(timedelta(hours=7))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
)

EXPORT_TEMPLATE_ID = "4e9c2265-c90e-456a-a5d7-5c7fe09342e5"
EXPORT_VARIANT_ID = "7F0000010175551BAE4736E652E83540"
DOWNLOAD_DIR = Path("../bao-cao-ca")


def load_json_file(file_name: str):
    base_dir = Path(__file__).resolve().parent
    file_path = base_dir / file_name
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_config():
    return load_json_file("config.json")


CONFIG = load_config()

if CONFIG.get("proxy"):
    os.environ["SE_PROXY"] = CONFIG["proxy"].strip()


def parse_page_id(report_url: str) -> str:
    parsed = urlparse(report_url)
    query = parse_qs(parsed.query)
    page_ids = query.get("pageId")
    if not page_ids:
        raise ValueError("Missing pageId trong URL")
    return page_ids[0]


def create_requests_session(jsessionid: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.cookies.set("JSESSIONID", jsessionid)
    return session


def create_edge_driver():
    options = Options()
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    return webdriver.Edge(options=options)


def login_sso_and_get_session_info() -> tuple[str, str]:
    driver = create_edge_driver()
    try:
        start_url = CONFIG.get("baocaoca_start_url", "").strip()
        if not start_url:
            raise ValueError("Thiếu baocaoca_start_url trong config.json")

        driver.get(start_url)

        print("Cửa sổ Edge đã mở.")
        print("Vui lòng đăng nhập SSO trên cửa sổ này, sau đó truy cập trang báo cáo cần export.")
        input("Khi đã mở đúng trang báo cáo, nhấn Enter để tiếp tục ...")

        report_url = driver.current_url
        if not report_url or "pageId=" not in report_url:
            raise ValueError(
                f"URL hiện tại không hợp lệ hoặc không chứa pageId: {report_url}"
            )

        WebDriverWait(driver, 30).until(
            lambda d: d.get_cookie("JSESSIONID") is not None
            or d.get_cookie("jsessionid") is not None
        )

        cookie = driver.get_cookie("JSESSIONID") or driver.get_cookie("jsessionid")
        if not cookie or "value" not in cookie:
            raise ValueError("Không lấy được JSESSIONID sau khi đăng nhập SSO.")

        jsessionid = cookie["value"]

        print(f"Current report URL: {report_url}")

        return report_url, jsessionid

    finally:
        driver.quit()


def fetch_report_html(session: requests.Session, report_url: str) -> str:
    resp = session.get(report_url, timeout=60)
    resp.raise_for_status()
    return resp.text


def extract_staffs_from_html(html: str):
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    report_title = title_tag.get_text(strip=True) if title_tag else "Biên bản bàn giao"
    if not report_title:
        report_title = "Biên bản bàn giao"

    main_div = soup.select_one("div#main-content")
    if not main_div:
        raise ValueError("Không tìm thấy div#main-content")

    p_nodes = main_div.find_all("p", recursive=False)
    if len(p_nodes) < 2:
        raise ValueError("Không tìm thấy đủ 2 thẻ <p> trực tiếp trong div#main-content")

    def get_usernames(p_tag):
        usernames = []
        for a in p_tag.select("a[data-username]"):
            username = a.get("data-username")
            if username:
                usernames.append(username)
        return usernames

    prev_staffs_vec = get_usernames(p_nodes[0])
    curr_staffs_vec = get_usernames(p_nodes[1])

    return report_title, prev_staffs_vec, curr_staffs_vec


def resolve_sender_fullname(curr_staffs_vec: list[str]) -> str:
    sender_map = load_json_file("mail_sender_staffs.json")

    for username in curr_staffs_vec:
        if username in sender_map:
            return sender_map[username]

    raise ValueError(
        "Không tìm thấy username nào trong curr_staffs có trong file mail_sender_staffs.json."
    )


def build_export_payload(page_id: str) -> dict:
    return {
        "pageId": page_id,
        "pageSet": "descendants",
        "templateId": EXPORT_TEMPLATE_ID,
        "properties": {
            "labels": {
                "includeContentWithLabels": [],
                "excludeContentWithLabels": [],
                "indexTerms": [],
            },
            "content": {
                "links": ["enableExternalLinks", "enableConfluenceLinks"],
                "images": "fullResolution",
                "advanced": "enableHeadingPromotion",
                "comalaWorkflows": [],
            },
            "macros": {
                "macros": ["showTocOutput", "showChildrenOutput"],
            },
            "title": {
                "figure": "after",
                "table": "after",
            },
            "printOptions": {
                "artifactFileName": (
                    '<span contenteditable="false" draggable="false" class="template-placeholder" '
                    'data-placeholder-app-key="com.k15t.scroll.pdf" data-placeholder-key="document-title" '
                    'data-placeholder-velocity="${document.title}" data-placeholder-name="Document Title" '
                    'data-placeholder-properties="{}">Document Title</span>-v'
                    '<span contenteditable="false" draggable="false" class="template-placeholder" '
                    'data-placeholder-app-key="com.k15t.scroll.pdf" data-placeholder-key="document-revision" '
                    'data-placeholder-velocity="${document.rootPage.revision}" '
                    'data-placeholder-name="Document Revision" data-placeholder-properties="{}">'
                    'Document Revision</span>-'
                    '<span contenteditable="false" draggable="false" class="template-placeholder" '
                    'data-placeholder-app-key="com.k15t.scroll.pdf" data-placeholder-key="export-date" '
                    'data-placeholder-velocity="${export.date(&#x22;YYYMMdd_HHmmss&#x22;)}" '
                    'data-placeholder-name="Export Date (YYYMMdd_HHmmss)" '
                    'data-placeholder-properties="{&#x22;pattern&#x22;:&#x22;YYYMMdd_HHmmss&#x22;}">'
                    'Export Date (YYYMMdd_HHmmss)</span>'
                )
            },
            "locale": {"defaultLocale": "en"},
            "tables": {"tableFit": "AUTO_FIT_TO_WINDOW"},
        },
        "pageOptions": {"variantId": EXPORT_VARIANT_ID},
        "locale": "en-US",
        "debugMode": False,
    }


def start_export_job(session: requests.Session, page_id: str) -> str:
    export_api_url = CONFIG.get("baocaoca_export_api_url", "").strip()
    if not export_api_url:
        raise ValueError("Thiếu baocaoca_export_api_url trong config.json")

    payload = build_export_payload(page_id)

    resp = session.post(
        export_api_url,
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    print(f"Export status: {resp.status_code}")
    resp.raise_for_status()

    data = resp.json()
    job_id = data.get("exportJobId")
    if not job_id:
        raise ValueError("Missing exportJobId field")

    return job_id


def wait_for_download_url(session: requests.Session, job_id: str) -> str:
    export_api_url = CONFIG.get("baocaoca_export_api_url", "").strip()
    if not export_api_url:
        raise ValueError("Thiếu baocaoca_export_api_url trong config.json")

    status_url = f"{export_api_url}/{job_id}/status"

    while True:
        resp = session.get(status_url, timeout=60)
        print(f"Polling status: {resp.status_code}")
        resp.raise_for_status()

        data = resp.json()
        download_url = data.get("downloadUrl")
        if download_url:
            return download_url

        time.sleep(5)


def download_export_file(session: requests.Session, download_url: str) -> Path:
    resp = session.get(download_url, timeout=120)
    print(f"Download status: {resp.status_code}")
    resp.raise_for_status()

    filename = unquote(download_url.rstrip("/").split("/")[-1]) or "output.docx"

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_path = DOWNLOAD_DIR / filename

    with open(file_path, "wb") as f:
        f.write(resp.content)

    return file_path


def build_mail_subject_and_shift():
    now = datetime.now(BANGKOK_TZ)
    hour = now.hour

    if 7 <= hour < 19:
        ca = 2
        ngay_bao_cao = (now.date() - timedelta(days=1)).strftime("%d/%m/%Y")
    else:
        ca = 1
        ngay_bao_cao = now.date().strftime("%d/%m/%Y")

    subject = f"Báo cáo FO 247 ca {ca} ngày {ngay_bao_cao}"
    return ca, ngay_bao_cao, subject


def build_email_message(
    prev_staffs: str,
    curr_staffs: str,
    sender_fullname: str,
    report_title: str,
    report_url: str,
    file_path: Path,
    receivers: list[str],
    mail_from: str,
) -> EmailMessage:
    ca, ngay_bao_cao, subject = build_mail_subject_and_shift()

    body_html = f"""
    <p>Dear các anh,</p>

    <p>Em gửi báo cáo trực ca {ca} ngày {ngay_bao_cao}.</p>

    <p>
        Người bàn giao: {prev_staffs}<br>
        Người nhận: {curr_staffs}<br>
        Biên bản bàn giao: <a href="{report_url}">{report_title}</a>
    </p>

    <p>
        Best regards,<br>
        <i>{sender_fullname.strip()}</i><br>
        ---
    </p>
    """

    msg = EmailMessage()
    msg["From"] = f"{sender_fullname.strip()} <{mail_from}>"
    msg["To"] = ", ".join(receivers)
    msg["Subject"] = subject
    msg.set_content("Vui lòng xem email ở dạng HTML.")
    msg.add_alternative(body_html, subtype="html")

    with open(file_path, "rb") as f:
        file_bytes = f.read()

    msg.add_attachment(
        file_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=file_path.name,
    )

    return msg


def send_handover_email(
    prev_staffs: str,
    curr_staffs: str,
    sender_fullname: str,
    report_title: str,
    report_url: str,
    file_path: Path,
    receivers: list[str],
):
    msg = build_email_message(
        prev_staffs=prev_staffs,
        curr_staffs=curr_staffs,
        sender_fullname=sender_fullname,
        report_title=report_title,
        report_url=report_url,
        file_path=file_path,
        receivers=receivers,
        mail_from=CONFIG["mail_from"],
    )

    with smtplib.SMTP_SSL(CONFIG["smtp_host"], 465, timeout=60) as server:
        server.login(
            CONFIG["smtp_username"],
            CONFIG["smtp_password"],
        )
        server.send_message(msg)


def run():
    report_url, jsessionid = login_sso_and_get_session_info()
    page_id = parse_page_id(report_url)

    session = create_requests_session(jsessionid)

    html = fetch_report_html(session, report_url)
    report_title, prev_staffs_vec, curr_staffs_vec = extract_staffs_from_html(html)

    if not prev_staffs_vec:
        print("Cảnh báo: prev_staffs rỗng", file=sys.stderr)
    if not curr_staffs_vec:
        print("Cảnh báo: curr_staffs rỗng", file=sys.stderr)

    prev_staffs = ", ".join(prev_staffs_vec)
    curr_staffs = ", ".join(curr_staffs_vec)

    sender_fullname = resolve_sender_fullname(curr_staffs_vec)

    receivers = CONFIG.get("mail_to", [])
    if not isinstance(receivers, list):
        raise ValueError("mail_to trong config.json phải là list")

    job_id = start_export_job(session, page_id)
    download_url = wait_for_download_url(session, job_id)
    file_path = download_export_file(session, download_url)

    print(f"File saved at: {file_path}")

    send_handover_email(
        prev_staffs=prev_staffs,
        curr_staffs=curr_staffs,
        sender_fullname=sender_fullname,
        report_title=report_title,
        report_url=report_url,
        file_path=file_path,
        receivers=receivers,
    )

    print("Gửi mail thành công")


if __name__ == "__main__":
    try:
        run()
    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        if e.response is not None:
            print(f"Response status: {e.response.status_code}", file=sys.stderr)
            try:
                print(f"Response body: {e.response.text[:2000]}", file=sys.stderr)
            except Exception:
                pass
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)