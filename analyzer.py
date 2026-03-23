from pathlib import Path
from collections import Counter
from openpyxl import load_workbook

def input_folder_path() -> Path:
    while True:
        folder = input("Nhập đường dẫn thư mục chứa file xlsx: ").strip().strip('"')
        path = Path(folder)
        if path.exists() and path.is_dir():
            return path
        print("Thư mục không tồn tại hoặc không hợp lệ. Vui lòng nhập lại.")


def input_top_k() -> int:
    while True:
        value = input("Nhập top k muốn hiển thị: ").strip()
        if value.isdigit() and int(value) > 0:
            return int(value)
        print("Vui lòng nhập số nguyên dương.")


def normalize_cell_value(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def process_excel_file(file_path: Path):
    counter_e = Counter()
    counter_f = Counter()
    counter_h = Counter()
    total_rows = 0

    wb = load_workbook(file_path, read_only=True, data_only=True)
    try:
        for sheet in wb.worksheets:
            for row in sheet.iter_rows(min_row=12, min_col=5, max_col=8, values_only=True):
                # row sẽ gồm: E, F, G, H
                val_e = normalize_cell_value(row[0])
                val_f = normalize_cell_value(row[1])
                val_h = normalize_cell_value(row[3])

                # Đếm tổng số dòng dữ liệu nếu trong 3 cột E/F/H có ít nhất 1 giá trị
                if val_e is not None or val_f is not None or val_h is not None:
                    total_rows += 1

                if val_e is not None:
                    counter_e.update([val_e])

                if val_f is not None:
                    counter_f.update([val_f])

                if val_h is not None:
                    counter_h.update([val_h])
    finally:
        wb.close()

    return counter_e, counter_f, counter_h, total_rows


def print_top(counter: Counter, title: str, top_k: int):
    print(f"\n{title}")
    print("-" * len(title))

    if not counter:
        print("Không có dữ liệu.")
        return

    for idx, (value, count) in enumerate(counter.most_common(top_k), start=1):
        print(f"{idx:>2}. {value} -> {count}")


def main():
    folder_path = input_folder_path()
    top_k = input_top_k()

    excel_files = list(folder_path.rglob("*.xlsx"))
    if not excel_files:
        print(f"Không tìm thấy file .xlsx nào trong thư mục: {folder_path}")
        return

    total_counter_e = Counter()
    total_counter_f = Counter()
    total_counter_h = Counter()
    grand_total_rows = 0

    print(f"\nTìm thấy {len(excel_files)} file .xlsx\n")

    for file_path in excel_files:
        try:
            counter_e, counter_f, counter_h, file_rows = process_excel_file(file_path)

            total_counter_e.update(counter_e)
            total_counter_f.update(counter_f)
            total_counter_h.update(counter_h)
            grand_total_rows += file_rows

            print(f"Đã xử lý: {file_path} | Số dòng dữ liệu từ dòng 12 trở đi: {file_rows}")
        except Exception as e:
            print(f"Lỗi khi đọc file {file_path}: {e}")

    print("\n" + "=" * 60)
    print(f"TỔNG số dòng dữ liệu từ dòng 12 trở đi ở tất cả file: {grand_total_rows}")
    print("=" * 60)

    print_top(total_counter_e, f"TOP {top_k} IPs", top_k)
    print_top(total_counter_f, f"TOP {top_k} Nations", top_k)
    print_top(total_counter_h, f"TOP {top_k} URLs", top_k)


if __name__ == "__main__":
    main()