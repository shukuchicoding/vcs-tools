import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime


def valid_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Ngày không hợp lệ: '{value}'. Định dạng đúng là YYYY-MM-DD"
        ) from exc


def valid_top_k(value: str) -> int:
    try:
        k = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--top-k phải là số nguyên dương") from exc

    if k <= 0:
        raise argparse.ArgumentTypeError("--top-k phải lớn hơn 0")

    return k


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export report theo khoảng ngày rồi chạy analyzer"
    )
    parser.add_argument(
        "--customer_account",
        nargs="+",
        required=True,
        help="Một hoặc nhiều tài khoản customer, ví dụ: --customer_account abc def",
    )
    parser.add_argument(
        "--start_date",
        required=True,
        type=valid_date,
        help="Ngày bắt đầu, định dạng YYYY-MM-DD",
    )
    parser.add_argument(
        "--end_date",
        required=True,
        type=valid_date,
        help="Ngày kết thúc, định dạng YYYY-MM-DD",
    )
    parser.add_argument(
        "--top-k",
        required=True,
        type=valid_top_k,
        help="Số lượng top kết quả cần phân tích",
    )
    parser.add_argument(
        "--from",
        dest="run_from",
        required=True,
        choices=["DESKTOP", "LAPTOP"],
        help="Nguồn chạy script: DESKTOP hoặc LAPTOP",
    )

    args = parser.parse_args()

    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        parser.error("--start_date phải nhỏ hơn hoặc bằng --end_date")

    return args


def run_command(cmd: list[str], step_name: str):
    print(f"\n=== {step_name} ===")
    print("Running:", " ".join(cmd))

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Bước '{step_name}' thất bại với mã thoát {result.returncode}"
        )


def remove_reports_folder(folder: Path):
    if folder.exists():
        print(f"Xóa thư mục báo cáo cũ: {folder.resolve()}")
        shutil.rmtree(folder)


def build_reports_folders(customer_accounts: list[str]) -> list[Path]:
    return [Path("..") / "reports" / customer_account for customer_account in customer_accounts]


def main():
    args = parse_args()

    customer_accounts = args.customer_account
    start_date = args.start_date
    end_date = args.end_date
    top_k = args.top_k
    run_from = args.run_from

    reports_folders = build_reports_folders(customer_accounts)

    for reports_folder in reports_folders:
        remove_reports_folder(reports_folder)

    export_cmd = [
        sys.executable,
        ".\\all_domains_report_downloader.py",
        "--customer_account",
        *customer_accounts,
        "--start_date",
        start_date,
        "--end_date",
        end_date,
        "--from",
        run_from,
    ]

    analyze_cmd = [
        sys.executable,
        ".\\analyzer.py",
        "--folder",
        *[str(folder) for folder in reports_folders],
        "--top-k",
        str(top_k),
    ]

    run_command(export_cmd, "Export report theo khoảng ngày")
    run_command(analyze_cmd, "Phân tích report")

    print("\nHoàn thành toàn bộ quy trình.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)