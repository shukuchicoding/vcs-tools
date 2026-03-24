import argparse
import calendar
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
)


def valid_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Ngày không hợp lệ: '{value}'. Định dạng đúng là YYYY-MM-DD"
        )


def valid_month(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m")
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Tháng không hợp lệ: '{value}'. Định dạng đúng là YYYY-MM"
        )


def month_date_range(month_value: str) -> tuple[str, str]:
    month_dt = datetime.strptime(month_value, "%Y-%m")
    last_day = calendar.monthrange(month_dt.year, month_dt.month)[1]
    return (
        month_dt.strftime("%Y-%m-01"),
        month_dt.replace(day=last_day).strftime("%Y-%m-%d"),
    )


def iter_dates_in_month(month_value: str):
    start_date, end_date = month_date_range(month_value)
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file config: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    export_base_url = str(data.get("export_base_url", "")).strip()
    if not export_base_url:
        raise ValueError("Thiếu 'export_base_url' trong config.json")

    return data


def get_cookie_domain_from_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"export_base_url không hợp lệ: {base_url}")

    host = parsed.hostname
    if not host:
        raise ValueError(f"Không lấy được hostname từ export_base_url: {base_url}")

    return host


def parse_args():
    parser = argparse.ArgumentParser(description="Export customer report")
    parser.add_argument("--customer_account", required=True, help="Tên tài khoản customer")
    parser.add_argument("--start_date", type=valid_date, help="Ngày bắt đầu YYYY-MM-DD")
    parser.add_argument("--end_date", type=valid_date, help="Ngày kết thúc YYYY-MM-DD")
    parser.add_argument("--month", type=valid_month, help="Export theo tháng YYYY-MM")
    parser.add_argument(
        "--report_type",
        choices=["events", "attacks"],
        help="Loại báo cáo: events hoặc attacks. Mặc định: export cả hai",
    )
    parser.add_argument(
        "--proxy",
        help="Proxy dùng cho requests và Selenium, ví dụ: http://192.168.5.8:3128",
    )
    parser.add_argument("--config", default="config.json", help="Đường dẫn file config.json")

    args = parser.parse_args()

    if args.month:
        if args.start_date or args.end_date:
            parser.error("Khi dùng --month thì không truyền --start_date hoặc --end_date")
    else:
        if not args.start_date or not args.end_date:
            parser.error("Phải truyền đủ --start_date và --end_date nếu không dùng --month")

        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
        if start_dt > end_dt:
            parser.error("--start_date phải nhỏ hơn hoặc bằng --end_date")

    return args


class CloudClient:
    def __init__(
        self,
        customer_account: str,
        start_date: str | None,
        end_date: str | None,
        report_type: str,
        base_url: str,
        proxy: str | None = None,
    ):
        self.customer_account = customer_account
        self.start_date = start_date
        self.end_date = end_date
        self.report_type = report_type
        self.base_url = base_url.rstrip("/")
        self.proxy = proxy.strip() if proxy else None
        self.cookie_domain = get_cookie_domain_from_base_url(self.base_url)

        self.login_url = f"{self.base_url}/admin/#/orders"
        self.customer_api_url = f"{self.base_url}/admin_api/v1/customer/"
        self.domain_api_url = f"{self.base_url}/admin_api/v1/domain/website/"
        self.export_events_api_url = (
            f"{self.base_url}/admin_waf/api/v1/customer-report/export-customer-event/"
        )
        self.export_attacks_api_url = (
            f"{self.base_url}/admin_api/v1/customer-report/export-attacks-report/"
        )

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

        if self.proxy:
            self.session.proxies.update({
                "http": self.proxy,
                "https": self.proxy,
            })

        self.d1n = None
        self.jsessionid = None
        self.customer_id = None
        self.distributor_id = None
        self.domain_names = []

    def _build_edge_options(self, proxy: str | None = None) -> Options:
        options = Options()
        options.add_argument("--log-level=3")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        if proxy:
            options.add_argument(f"--proxy-server={proxy}")
        return options

    def _create_driver(self):
        try:
            return webdriver.Edge(options=self._build_edge_options(proxy=self.proxy))
        except Exception as error:
            mode = f"proxy '{self.proxy}'" if self.proxy else "không dùng proxy"
            raise RuntimeError(
                f"Khởi tạo Edge driver thất bại khi {mode}. Chi tiết: {error}"
            ) from error

    def login_and_capture_cookies(self):
        driver = self._create_driver()
        try:
            driver.get(self.login_url)
            print("Đăng nhập trên cửa sổ Edge vừa mở.")
            input("Đăng nhập xong, nhấn Enter để tiếp tục ...")
            WebDriverWait(driver, 30).until(lambda d: len(d.get_cookies()) > 0)

            cookies = driver.get_cookies()
            jsessionid = None
            d1n = None

            for c in cookies:
                name = c.get("name", "").lower()
                if name == "jsessionid":
                    jsessionid = c.get("value")
                elif name == "d1n":
                    d1n = c.get("value")

            if not d1n:
                match = re.search(r"D1N=([a-fA-F0-9]+)", driver.page_source or "")
                if match:
                    d1n = match.group(1)

            if not jsessionid:
                raise ValueError("Không lấy được JSESSIONID từ phiên Selenium.")
            if not d1n:
                raise ValueError("Không lấy được D1N từ phiên Selenium/HTML.")

            self.jsessionid = jsessionid
            self.d1n = d1n
            self.session.cookies.set("D1N", self.d1n, domain=self.cookie_domain, path="/")
            self.session.cookies.set("JSESSIONID", self.jsessionid, domain=self.cookie_domain, path="/")
        finally:
            driver.quit()

    def get_customer_info(self):
        if not self.d1n or not self.jsessionid:
            raise ValueError("Chưa có D1N/JSESSIONID. Hãy gọi login_and_capture_cookies() trước.")

        resp = self.session.get(
            self.customer_api_url,
            params={"user_name": self.customer_account, "_fields": "user_name"},
            timeout=30,
        )
        resp.raise_for_status()

        rows = resp.json().get("data", {}).get("rows", [])
        if not rows:
            raise ValueError(f"Không tìm thấy customer cho user_name={self.customer_account}")

        row = rows[0]
        self.customer_id = row.get("_id")
        self.distributor_id = row.get("distributor_id")

        if not self.customer_id or not self.distributor_id:
            raise ValueError("Thiếu customer_id hoặc distributor_id trong response.")

    def get_domains(self):
        if not self.distributor_id:
            raise ValueError("distributor_id chưa có. Hãy gọi get_customer_info() trước.")

        resp = self.session.get(
            self.domain_api_url,
            params={
                "distributor_id": self.distributor_id,
                "user_name": self.customer_account,
            },
            timeout=30,
        )
        resp.raise_for_status()

        domain_names = resp.json().get("data", [])
        if not isinstance(domain_names, list):
            raise ValueError("Trường 'data' không phải list domain.")

        self.domain_names = domain_names
        print(f"domains ({len(self.domain_names)}): {self.domain_names}")

    def prepare(self):
        self.login_and_capture_cookies()
        self.get_customer_info()
        self.get_domains()

    def get_export_url(self) -> str:
        return self.export_events_api_url if self.report_type == "events" else self.export_attacks_api_url

    def build_export_payload(self) -> dict:
        if not self.start_date or not self.end_date:
            raise ValueError("Thiếu start_date hoặc end_date.")

        payload = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "customer_id": self.customer_id,
            "domain_names": self.domain_names,
            "distributor_id": self.distributor_id,
            "lang": "vi",
        }
        if self.report_type == "events":
            payload["action"] = ["block"]
            payload["protect_mode"] = ["on"]
        return payload

    def build_export_cookies(self) -> dict:
        cookies = {
            "D1N": self.d1n,
            "JSESSIONID": self.jsessionid,
            "cp_distid": str(self.distributor_id),
        }
        if self.report_type == "events":
            cookies["customer_id"] = str(self.customer_id)
        return cookies

    def build_output_path(self) -> Path:
        if not self.start_date or not self.end_date:
            raise ValueError("Thiếu start_date hoặc end_date.")

        output_dir = Path("../reports") / self.customer_account
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = int(time.time() * 1000)
        ext = "xlsx" if self.report_type == "events" else "pdf"
        filename = (
            f"{self.report_type}_{self.customer_account}_{self.start_date}_{self.end_date}_{timestamp}.{ext}"
        )
        return output_dir / filename

    def export_report(self) -> Path:
        resp = self.session.post(
            self.get_export_url(),
            json=self.build_export_payload(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
            cookies=self.build_export_cookies(),
            timeout=120,
        )
        resp.raise_for_status()

        output_path = self.build_output_path()
        output_path.write_bytes(resp.content)
        print(f"Saved report: {output_path.resolve()}")
        return output_path


def resolve_report_dates(args, report_type: str) -> tuple[str, str]:
    if args.month:
        if report_type == "attacks":
            return month_date_range(args.month)

        first_day = next(iter_dates_in_month(args.month))
        return first_day, first_day

    return args.start_date, args.end_date


def run_export(client: CloudClient, args, report_type: str):
    client.report_type = report_type

    if args.month and report_type == "events":
        days = list(iter_dates_in_month(args.month))

        for idx, day in enumerate(days, start=1):
            client.start_date = day
            client.end_date = day
            print(f"Export events ngày {day} ...")
            client.export_report()

            if idx < len(days):
                time.sleep(10)

        print(f"Hoàn thành events. Tổng số file exported: {len(days)}")
        return

    start_date, end_date = resolve_report_dates(args, report_type)
    client.start_date = start_date
    client.end_date = end_date
    client.export_report()
    print(f"Hoàn thành {report_type}.")


def main():
    args = parse_args()
    config = load_config(Path(args.config))
    base_url = str(config["export_base_url"]).strip()

    report_types = [args.report_type] if args.report_type else ["events", "attacks"]

    initial_report_type = report_types[0]
    start_date, end_date = resolve_report_dates(args, initial_report_type)

    client = CloudClient(
        customer_account=args.customer_account,
        start_date=start_date,
        end_date=end_date,
        report_type=initial_report_type,
        base_url=base_url,
        proxy=args.proxy,
    )

    client.prepare()

    for report_type in report_types:
        print(f"=== Bắt đầu export {report_type} ===")
        run_export(client, args, report_type)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        if e.response is not None:
            print(f"Response status: {e.response.status_code}", file=sys.stderr)
            body_preview = e.response.text[:2000] if e.response.text else ""
            print(f"Response body: {body_preview}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)