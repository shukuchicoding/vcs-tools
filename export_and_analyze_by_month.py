import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime


def valid_month(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m")
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Tháng không hợp lệ: '{value}'. Định dạng đúng là YYYY-MM"
        )


def valid_top_k(value: str) -> int:
    try:
        k = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("--top-k phải là số nguyên dương")

    if k <= 0:
        raise argparse.ArgumentTypeError("--top-k phải lớn hơn 0")

    return k


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export report theo tháng rồi chạy analyzer"
    )
    parser.add_argument(
        "--customer_account",
        required=True,
        help="Tên tài khoản customer",
    )
    parser.add_argument(
        "--month",
        required=True,
        type=valid_month,
        help="Tháng cần export, định dạng YYYY-MM",
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
        choices=["NOC-PC", "NOC-LAPTOP"],
        help="Nguồn chạy script: NOC-PC hoặc NOC-LAPTOP",
    )
    return parser.parse_args()


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


def main():
    args = parse_args()

    customer_account = args.customer_account
    month = args.month
    top_k = args.top_k
    run_from = args.run_from

    reports_folder = Path("..") / "reports" / customer_account

    remove_reports_folder(reports_folder)

    export_cmd = [
        sys.executable,
        ".\\all_domains_report_downloader.py",
        "--customer_account",
        customer_account,
        "--month",
        month,
        "--from",
        run_from,
    ]

    analyze_cmd = [
        sys.executable,
        ".\\analyzer.py",
        "--folder",
        str(reports_folder),
        "--top-k",
        str(top_k),
    ]

    run_command(export_cmd, "Export report theo tháng")
    run_command(analyze_cmd, "Phân tích report")

    print("\nHoàn thành toàn bộ quy trình.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)