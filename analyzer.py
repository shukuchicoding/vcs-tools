from pathlib import Path
from collections import Counter, defaultdict
from openpyxl import load_workbook
import argparse
import sys

def parse_args():
    parser = argparse.ArgumentParser(
        description="Tổng hợp báo cáo."
    )
    parser.add_argument(
        "--folder",
        required=True,
        type=str,
        help="Đường dẫn thư mục chứa file báo cáo",
    )
    parser.add_argument(
        "--top-k",
        required=True,
        type=int,
        help="Số lượng top muốn hiển thị",
    )
    parser.add_argument(
        "--report-type",
        required=True,
        choices=["events", "attacks"],
        help="Loại báo cáo: events hoặc attacks",
    )

    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.exists() or not folder.is_dir():
        parser.error(f"Thư mục không tồn tại hoặc không hợp lệ: {folder}")

    if args.top_k <= 0:
        parser.error("--top-k phải là số nguyên dương.")

    return folder, args.top_k, args.report_type

def build_output_file(folder_path: Path, report_type: str) -> Path:
    folder_name = folder_path.name.strip()
    if not folder_name:
        folder_name = "default"
    return folder_path / f"{folder_name}_{report_type}_report.txt"

def normalize_cell_value(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None

def process_excel_file(file_path: Path):
    counter_ip = Counter()
    counter_url = Counter()
    ip_nation_map = defaultdict(Counter)
    total_rows = 0

    wb = load_workbook(file_path, read_only=True, data_only=True)
    try:
        for sheet in wb.worksheets:
            for row in sheet.iter_rows(min_row=12, min_col=5, max_col=8, values_only=True):
                # row gồm: E, F, G, H
                val_ip = normalize_cell_value(row[0])      # cột E
                val_nation = normalize_cell_value(row[1])  # cột F
                val_url = normalize_cell_value(row[3])     # cột H

                if val_ip is not None or val_nation is not None or val_url is not None:
                    total_rows += 1

                if val_ip is not None:
                    counter_ip.update([val_ip])

                if val_url is not None:
                    counter_url.update([val_url])

                if val_ip is not None and val_nation is not None:
                    ip_nation_map[val_ip].update([val_nation])
    finally:
        wb.close()

    return counter_ip, counter_url, ip_nation_map, total_rows

def process_pdf_file(file_path: Path):
    """
    Placeholder cho report_type='attacks'.
    Bạn sẽ tự bổ sung logic xử lý PDF sau.
    """
    raise NotImplementedError("Chưa implement xử lý PDF cho report_type='attacks'")

def get_top_ip_with_nation(counter_ip: Counter, ip_nation_map: dict, top_k: int):
    result = []
    for ip, count in counter_ip.most_common(top_k):
        nation_counter = ip_nation_map.get(ip, Counter())
        if nation_counter:
            nation, nation_count = nation_counter.most_common(1)[0]
        else:
            nation, nation_count = "Unknown", 0

        result.append({
            "ip": ip,
            "count": count,
            "nation": nation,
            "nation_count_for_ip": nation_count,
        })
    return result

def format_top_ip_with_nation(top_ip_data):
    lines = []
    title = "TOP IPs và quốc gia tương ứng"
    lines.append(f"\n{title}")
    lines.append("-" * len(title))

    if not top_ip_data:
        lines.append("Không có dữ liệu.")
        return lines

    for idx, item in enumerate(top_ip_data, start=1):
        lines.append(
            f"{idx:>2}. IP: {item['ip']} -> {item['count']} | Nation: {item['nation']}"
        )
    return lines

def format_top(counter: Counter, title: str, top_k: int):
    lines = []
    lines.append(f"\n{title}")
    lines.append("-" * len(title))

    if not counter:
        lines.append("Không có dữ liệu.")
        return lines

    for idx, (value, count) in enumerate(counter.most_common(top_k), start=1):
        lines.append(f"{idx:>2}. {value} -> {count}")
    return lines

def handle_events(folder_path: Path, top_k: int, output_file: Path):
    excel_files = list(folder_path.rglob("*.xlsx"))
    if not excel_files:
        print(f"Không tìm thấy file .xlsx nào trong thư mục: {folder_path}")
        sys.exit(0)

    total_counter_ip = Counter()
    total_counter_url = Counter()
    total_ip_nation_map = defaultdict(Counter)
    grand_total_rows = 0

    console_lines = []
    console_lines.append(f"\nTổng hợp {len(excel_files)} file .xlsx\n")

    for file_path in excel_files:
        try:
            counter_ip, counter_url, ip_nation_map, file_rows = process_excel_file(file_path)

            total_counter_ip.update(counter_ip)
            total_counter_url.update(counter_url)
            grand_total_rows += file_rows

            for ip, nation_counter in ip_nation_map.items():
                total_ip_nation_map[ip].update(nation_counter)

            console_lines.append(f"Đã xử lý: {file_path} | Số event: {file_rows}")
        except Exception as e:
            console_lines.append(f"Lỗi khi đọc file {file_path}: {e}")

    top_ip_data = get_top_ip_with_nation(total_counter_ip, total_ip_nation_map, top_k)

    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append(f"TỔNG số events ở tất cả file: {grand_total_rows}")
    report_lines.append("=" * 60)
    report_lines.extend(format_top_ip_with_nation(top_ip_data))
    report_lines.extend(format_top(total_counter_url, f"TOP {top_k} URLs", top_k))

    for line in console_lines:
        print(line)
    print()
    for line in report_lines:
        print(line)

    with open(output_file, "w", encoding="utf-8") as f:
        for line in console_lines:
            f.write(line + "\n")
        f.write("\n")
        for line in report_lines:
            f.write(line + "\n")

    print(f"\nĐã ghi report ra file: {output_file}")

def handle_attacks(folder_path: Path, top_k: int, output_file: Path):
    pdf_files = list(folder_path.rglob("*.pdf"))
    if not pdf_files:
        print(f"Không tìm thấy file .pdf nào trong thư mục: {folder_path}")
        sys.exit(0)

    console_lines = []
    console_lines.append(f"\nTổng hợp {len(pdf_files)} file .pdf\n")

    for file_path in pdf_files:
        try:
            process_pdf_file(file_path)
            console_lines.append(f"Đã phát hiện file PDF: {file_path}")
        except NotImplementedError as e:
            console_lines.append(f"Chưa xử lý file {file_path}: {e}")
        except Exception as e:
            console_lines.append(f"Lỗi khi đọc file {file_path}: {e}")

    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append("REPORT TYPE: ATTACKS")
    report_lines.append("Chưa implement logic xử lý PDF.")
    report_lines.append("=" * 60)

    for line in console_lines:
        print(line)
    print()
    for line in report_lines:
        print(line)

    with open(output_file, "w", encoding="utf-8") as f:
        for line in console_lines:
            f.write(line + "\n")
        f.write("\n")
        for line in report_lines:
            f.write(line + "\n")

    print(f"\nĐã ghi report ra file: {output_file}")

def main():
    folder_path, top_k, report_type = parse_args()
    output_file = build_output_file(folder_path, report_type)

    if report_type == "events":
        handle_events(folder_path, top_k, output_file)
    elif report_type == "attacks":
        handle_attacks(folder_path, top_k, output_file)
    else:
        print(f"report_type không hợp lệ: {report_type}")
        sys.exit(1)

if __name__ == "__main__":
    main()