import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
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

REPORT_TYPES = ("events", "attacks")
RUN_FROM_CHOICES = ("DESKTOP", "LAPTOP")
EVENT_EXPORT_DELAY_SECONDS = 10


def valid_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Ngày không hợp lệ: '{value}'. Định dạng đúng là YYYY-MM-DD"
        ) from exc


def iter_dates_in_range(start_date: str, end_date: str):
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file config: {config_path}")

    with open(config_path, "r", encoding="utf-8") as file:
        config = json.load(file)

    export_base_url = str(config.get("export_base_url", "")).strip()
    if not export_base_url:
        raise ValueError("Thiếu 'export_base_url' trong config.json")

    return config


def get_cookie_domain_from_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"export_base_url không hợp lệ: {base_url}")

    host = parsed.hostname
    if not host:
        raise ValueError(f"Không lấy được hostname từ export_base_url: {base_url}")

    return host


def resolve_proxy(run_from: str, config: dict) -> str | None:
    if run_from == "DESKTOP":
        proxy = str(config.get("proxy", "")).strip()
        if not proxy:
            raise ValueError("Thiếu 'proxy' trong config.json khi chạy với --from DESKTOP")
        return proxy

    return None


def parse_args():
    parser = argparse.ArgumentParser(description="Xuất báo cáo cho khách hàng")
    parser.add_argument(
        "--customer_account",
        nargs="+",
        required=True,
        help="Danh sách tài khoản customer (tra cứu chính xác trên portal), ví dụ: --customer_account abc def",
    )
    parser.add_argument(
        "--start_date",
        type=valid_date,
        required=True,
        help="Ngày bắt đầu YYYY-MM-DD",
    )
    parser.add_argument(
        "--end_date",
        type=valid_date,
        required=True,
        help="Ngày kết thúc YYYY-MM-DD",
    )
    parser.add_argument(
        "--report_type",
        choices=REPORT_TYPES,
        help="Loại báo cáo: events hoặc attacks. Mặc định: export cả hai",
    )
    parser.add_argument(
        "--from",
        dest="run_from",
        required=True,
        choices=RUN_FROM_CHOICES,
        help="Nguồn chạy script: DESKTOP hoặc LAPTOP",
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Đường dẫn file config.json",
    )

    args = parser.parse_args()

    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        parser.error("--start_date phải nhỏ hơn hoặc bằng --end_date")

    return args


@dataclass
class CustomerContext:
    customer_account: str
    customer_id: str
    distributor_id: str
    domain_names: list[str]


class AuthenticatedSession:
    def __init__(self, base_url: str, proxy: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.proxy = proxy.strip() if proxy else None
        self.cookie_domain = get_cookie_domain_from_base_url(self.base_url)

        self.login_url = f"{self.base_url}/admin/#/orders"

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

        if self.proxy:
            self.session.proxies.update({
                "http": self.proxy,
                "https": self.proxy,
            })

        self.jsessionid: str | None = None
        self.d1n: str | None = None

    def _build_edge_options(self) -> Options:
        options = Options()
        options.add_argument("--log-level=3")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        if self.proxy:
            options.add_argument(f"--proxy-server={self.proxy}")
        return options

    def _create_driver(self):
        try:
            return webdriver.Edge(options=self._build_edge_options())
        except Exception as exc:
            mode = f"proxy '{self.proxy}'" if self.proxy else "không dùng proxy"
            raise RuntimeError(
                f"Khởi tạo Edge driver thất bại khi {mode}. Chi tiết: {exc}"
            ) from exc

    def _apply_auth_cookies(self):
        if not self.jsessionid or not self.d1n:
            raise ValueError("Thiếu JSESSIONID hoặc D1N để apply vào session.")

        self.session.cookies.set("D1N", self.d1n, domain=self.cookie_domain, path="/")
        self.session.cookies.set(
            "JSESSIONID",
            self.jsessionid,
            domain=self.cookie_domain,
            path="/",
        )

    def login(self):
        driver = self._create_driver()
        try:
            driver.get(self.login_url)
            print("Đăng nhập trên cửa sổ Edge vừa mở.")
            input("Đăng nhập xong, nhấn Enter để tiếp tục ...")
            WebDriverWait(driver, 30).until(lambda d: len(d.get_cookies()) > 0)

            cookies = driver.get_cookies()
            jsessionid = None
            d1n = None

            for cookie in cookies:
                name = cookie.get("name", "").lower()
                if name == "jsessionid":
                    jsessionid = cookie.get("value")
                elif name == "d1n":
                    d1n = cookie.get("value")

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
            self._apply_auth_cookies()
        finally:
            driver.quit()


class CustomerResolver:
    def __init__(self, auth: AuthenticatedSession):
        self.auth = auth
        self.customer_api_url = f"{self.auth.base_url}/admin_api/v1/customer/"
        self.domain_api_url = f"{self.auth.base_url}/admin_api/v1/domain/website/"

    def resolve(self, customer_account: str) -> CustomerContext:
        customer_response = self.auth.session.get(
            self.customer_api_url,
            params={"user_name": customer_account, "_fields": "user_name"},
            timeout=30,
        )
        customer_response.raise_for_status()

        rows = customer_response.json().get("data", {}).get("rows", [])
        if not rows:
            raise ValueError(f"Không tìm thấy customer cho user_name={customer_account}")

        row = rows[0]
        customer_id = row.get("_id")
        distributor_id = row.get("distributor_id")

        if not customer_id or not distributor_id:
            raise ValueError(
                f"Thiếu customer_id hoặc distributor_id cho user_name={customer_account}"
            )

        domain_response = self.auth.session.get(
            self.domain_api_url,
            params={
                "distributor_id": distributor_id,
                "user_name": customer_account,
            },
            timeout=30,
        )
        domain_response.raise_for_status()

        domain_names = domain_response.json().get("data", [])
        if not isinstance(domain_names, list):
            raise ValueError(
                f"Trường 'data' không phải list domain cho user_name={customer_account}"
            )

        print(f"domains ({len(domain_names)}) của {customer_account}: {domain_names}")

        return CustomerContext(
            customer_account=customer_account,
            customer_id=customer_id,
            distributor_id=distributor_id,
            domain_names=domain_names,
        )


class ReportExporter:
    def __init__(self, auth: AuthenticatedSession):
        self.auth = auth
        self.export_urls = {
            "events": (
                f"{self.auth.base_url}/admin_waf/api/v1/customer-report/"
                "export-customer-event/"
            ),
            "attacks": (
                f"{self.auth.base_url}/admin_api/v1/customer-report/"
                "export-attacks-report/"
            ),
        }

    def _build_export_payload(
        self,
        context: CustomerContext,
        report_type: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        payload = {
            "start_date": start_date,
            "end_date": end_date,
            "customer_id": context.customer_id,
            "domain_names": context.domain_names,
            "distributor_id": context.distributor_id,
            "lang": "vi",
        }

        if report_type == "events":
            payload["action"] = ["block"]
            payload["protect_mode"] = ["on"]

        return payload

    def _build_export_cookies(
        self,
        context: CustomerContext,
        report_type: str,
    ) -> dict:
        if not self.auth.jsessionid or not self.auth.d1n:
            raise ValueError("Session chưa được login. Hãy gọi auth.login() trước.")

        cookies = {
            "D1N": self.auth.d1n,
            "JSESSIONID": self.auth.jsessionid,
            "cp_distid": str(context.distributor_id),
        }

        if report_type == "events":
            cookies["customer_id"] = str(context.customer_id)

        return cookies

    def _build_output_path(
        self,
        context: CustomerContext,
        report_type: str,
        start_date: str,
        end_date: str,
    ) -> Path:
        output_dir = Path("../reports") / context.customer_account
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = int(time.time() * 1000)
        extension = "xlsx" if report_type == "events" else "pdf"
        filename = (
            f"{report_type}_{context.customer_account}_{start_date}_{end_date}_{timestamp}.{extension}"
        )
        return output_dir / filename

    def export_once(
        self,
        context: CustomerContext,
        report_type: str,
        start_date: str,
        end_date: str,
    ) -> Path:
        response = self.auth.session.post(
            self.export_urls[report_type],
            json=self._build_export_payload(context, report_type, start_date, end_date),
            headers={
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
            cookies=self._build_export_cookies(context, report_type),
            timeout=120,
        )
        response.raise_for_status()

        output_path = self._build_output_path(context, report_type, start_date, end_date)
        output_path.write_bytes(response.content)
        print(f"Saved report: {output_path.resolve()}")
        return output_path

    def export_events_by_day(
        self,
        context: CustomerContext,
        start_date: str,
        end_date: str,
    ):
        days = list(iter_dates_in_range(start_date, end_date))

        for index, day in enumerate(days, start=1):
            print(f"Export events cho {context.customer_account} ngày {day} ...")
            self.export_once(context, "events", day, day)

            if index < len(days):
                time.sleep(EVENT_EXPORT_DELAY_SECONDS)

        print(
            f"Hoàn thành events cho {context.customer_account}. "
            f"Tổng số file exported: {len(days)}"
        )

    def export_attacks(
        self,
        context: CustomerContext,
        start_date: str,
        end_date: str,
    ):
        print(
            f"Export attacks cho {context.customer_account} "
            f"từ {start_date} đến {end_date} ..."
        )
        self.export_once(context, "attacks", start_date, end_date)
        print(f"Hoàn thành attacks cho {context.customer_account}.")

    def export(
        self,
        context: CustomerContext,
        report_types: list[str],
        start_date: str,
        end_date: str,
    ):
        for report_type in report_types:
            print(
                f"=== Bắt đầu export {report_type} "
                f"cho customer {context.customer_account} ==="
            )

            if report_type == "events":
                self.export_events_by_day(context, start_date, end_date)
            else:
                self.export_attacks(context, start_date, end_date)


def main():
    args = parse_args()
    config = load_config(Path(args.config))
    proxy = resolve_proxy(args.run_from, config)
    base_url = str(config["export_base_url"]).strip()

    report_types = [args.report_type] if args.report_type else list(REPORT_TYPES)

    auth = AuthenticatedSession(base_url=base_url, proxy=proxy)
    auth.login()

    resolver = CustomerResolver(auth)
    exporter = ReportExporter(auth)

    for customer_account in args.customer_account:
        print(f"================ CUSTOMER: {customer_account} ================")
        context = resolver.resolve(customer_account)
        exporter.export(context, report_types, args.start_date, args.end_date)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        if exc.response is not None:
            print(f"Response status: {exc.response.status_code}", file=sys.stderr)
            body_preview = exc.response.text[:2000] if exc.response.text else ""
            print(f"Response body: {body_preview}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)