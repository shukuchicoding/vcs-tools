import argparse
import json
import re
import sys
import time
from datetime import datetime
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
    parser = argparse.ArgumentParser(
        description="Export customer report"
    )
    parser.add_argument("--customer_account", required=True, help="Tên tài khoản customer")
    parser.add_argument(
        "--start_date",
        required=True,
        type=valid_date,
        help="Ngày bắt đầu, định dạng YYYY-MM-DD, ví dụ: 2026-03-01",
    )
    parser.add_argument(
        "--end_date",
        required=True,
        type=valid_date,
        help="Ngày kết thúc, định dạng YYYY-MM-DD, ví dụ: 2026-03-20",
    )
    parser.add_argument(
        "--report_type",
        required=True,
        choices=["events", "attacks"],
        help="Loại báo cáo: events hoặc attacks",
    )
    parser.add_argument(
        "--proxy",
        required=False,
        help="Proxy dùng cho requests và Selenium, ví dụ: http://192.168.5.8:3128",
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


class CloudClient:
    def __init__(
        self,
        customer_account: str,
        start_date: str,
        end_date: str,
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
        """
        Nếu có self.proxy thì khởi tạo Edge luôn với proxy.
        Nếu không có self.proxy thì khởi tạo Edge không proxy.
        Không dùng fallback.
        """
        try:
            options = self._build_edge_options(proxy=self.proxy)
            return webdriver.Edge(options=options)
        except Exception as error:
            if self.proxy:
                raise RuntimeError(
                    f"Khởi tạo Edge driver thất bại với proxy '{self.proxy}'. Chi tiết: {error}"
                ) from error

            raise RuntimeError(
                f"Khởi tạo Edge driver thất bại khi không dùng proxy. Chi tiết: {error}"
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
                page_source = driver.page_source or ""
                match = re.search(r"D1N=([a-fA-F0-9]+)", page_source)
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

        params = {
            "user_name": self.customer_account,
            "_fields": "user_name",
        }

        resp = self.session.get(
            self.customer_api_url,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()

        data = resp.json()
        rows = data.get("data", {}).get("rows", [])
        if not rows:
            raise ValueError(f"Không tìm thấy customer cho user_name={self.customer_account}")

        row = rows[0]
        self.customer_id = row.get("_id")
        self.distributor_id = row.get("distributor_id")

        if not self.customer_id or not self.distributor_id:
            raise ValueError("Thiếu customer_id hoặc distributor_id trong response.")

        return self.customer_id, self.distributor_id

    def get_domains(self):
        if not self.distributor_id:
            raise ValueError("distributor_id chưa có. Hãy gọi get_customer_info() trước.")

        params = {
            "distributor_id": self.distributor_id,
            "user_name": self.customer_account,
        }

        resp = self.session.get(
            self.domain_api_url,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()

        data = resp.json()
        domain_names = data.get("data", [])
        if not isinstance(domain_names, list):
            raise ValueError("Trường 'data' không phải list domain.")

        self.domain_names = domain_names
        print(f"domains ({len(self.domain_names)}): {self.domain_names}")
        return self.domain_names

    def get_export_url(self) -> str:
        if self.report_type == "events":
            return self.export_events_api_url
        return self.export_attacks_api_url

    def build_export_payload(self) -> dict:
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
        if self.report_type == "events":
            return {
                "D1N": self.d1n,
                "JSESSIONID": self.jsessionid,
                "cp_distid": str(self.distributor_id),
                "customer_id": str(self.customer_id),
            }

        return {
            "D1N": self.d1n,
            "JSESSIONID": self.jsessionid,
            "cp_distid": str(self.distributor_id),
        }

    def build_output_path(self) -> Path:
        output_dir = Path("../reports") / self.customer_account
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = int(time.time())

        if self.report_type == "events":
            filename = (
                f"events_{self.customer_account}_{self.start_date}_{self.end_date}_{timestamp}.xlsx"
            )
        else:
            filename = (
                f"attacks_{self.customer_account}_{self.start_date}_{self.end_date}_{timestamp}.pdf"
            )

        return output_dir / filename

    def export_report(self) -> Path:
        if not self.customer_id or not self.distributor_id:
            raise ValueError("Thiếu customer_id/distributor_id. Hãy gọi get_customer_info() trước.")
        if self.domain_names is None:
            raise ValueError("Danh sách domain chưa được khởi tạo. Hãy gọi get_domains() trước.")

        payload = self.build_export_payload()
        export_url = self.get_export_url()
        cookies = self.build_export_cookies()

        headers = {
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

        resp = self.session.post(
            export_url,
            json=payload,
            headers=headers,
            cookies=cookies,
            timeout=120,
        )
        resp.raise_for_status()

        output_path = self.build_output_path()
        with open(output_path, "wb") as f:
            f.write(resp.content)

        print(f"Saved report: {output_path.resolve()}")
        return output_path


def main():
    args = parse_args()
    config = load_config(Path(args.config))

    base_url = str(config["export_base_url"]).strip()

    client = CloudClient(
        customer_account=args.customer_account,
        start_date=args.start_date,
        end_date=args.end_date,
        report_type=args.report_type,
        base_url=base_url,
        proxy=args.proxy,
    )

    try:
        client.login_and_capture_cookies()
        client.get_customer_info()
        client.get_domains()
        client.export_report()
        print("Hoàn thành.")
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


if __name__ == "__main__":
    main()