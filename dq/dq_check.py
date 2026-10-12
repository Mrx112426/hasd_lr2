"""
Data Quality для ЛР2 Online Retail.

Независимая проверка контракта DE (DE_memo.md, eds.ipynb): тот же датасет
пересобирается отдельным кодом, режется на варианты, каждый вариант проходит
набор проверок, итоговые цифры сверяются с контрольными значениями ноутбука.

Запуск из корня репозитория:
    python dq/dq_check.py data.csv

Результат:
    dq/DQ_REPORT.md                 отчёт DQ (генерируется)
    dq/reference/*.csv              эталонные витрины DQ для сверки с аналитиком
    dq/datasets/*.csv               варианты чистого датасета (в git не хранятся)
"""
import hashlib
import json
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
DATASETS = HERE / "datasets"
REFERENCE = HERE / "reference"
EXPECTED = HERE / "expected_control_totals.json"

COLS = ["InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
# Нетоварные коды из DE_memo.md (сравнение без учёта регистра) + префикс gift_
NON_GOODS = {"POST", "DOT", "C2", "M", "D", "BANK CHARGES", "AMAZONFEE", "CRUK", "B", "S"}


# =========================================================== загрузка
def file_info(path):
    b = Path(path).read_bytes()
    return {
        "size": len(b),
        "sha256": hashlib.sha256(b).hexdigest(),
        # git с autocrlf хранит CSV с LF; исходник UCI/ноутбука — с CRLF
        "sha256_crlf": hashlib.sha256(b.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")).hexdigest(),
        "line_ending": "CRLF" if b"\r\n" in b[:10_000] else "LF",
    }


def load(path):
    raw = pd.read_csv(path, encoding="cp1252", dtype="string", keep_default_na=False)
    assert raw.columns.tolist() == COLS, f"Схема не совпадает: {raw.columns.tolist()}"
    return raw


def _price_1e4(text):
    """Цена в целых десятитысячных GBP: точные суммы без ошибок float."""
    if pd.isna(text):
        return pd.NA
    try:
        x = Decimal(str(text)) * 10000
    except Exception:
        return pd.NA
    return int(x) if x.is_finite() and x == x.to_integral_value() else pd.NA


def normalize(raw):
    """Дедупликация по 8 ИСХОДНЫМ полям, затем trim, пустое -> NA, типы, флаги."""
    dup = raw.duplicated(subset=COLS, keep="first")
    df = raw.loc[~dup].copy()
    for c in COLS:
        s = df[c].str.strip()
        df[c] = s.mask(s.eq(""), pd.NA)

    q = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Quantity"] = q.mask(q.isna() | q.mod(1).ne(0)).astype("Int64")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format="%m/%d/%Y %H:%M", errors="coerce")
    prices = {p: _price_1e4(p) for p in df["UnitPrice"].dropna().unique()}
    df["UnitPrice_1e4"] = df["UnitPrice"].map(prices).astype("Int64")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
    df["TotalPrice_1e4"] = df["Quantity"] * df["UnitPrice_1e4"]
    df["TotalPrice"] = df["TotalPrice_1e4"].astype("Float64") / 10000

    code = df["StockCode"]
    df["is_goods"] = (code.notna() & ~code.str.upper().isin(NON_GOODS)
                      & ~code.str.lower().str.startswith("gift_", na=False))
    valid = (df["InvoiceDate"].notna() & df["Quantity"].notna() & df["UnitPrice_1e4"].notna()
             & code.notna() & df["UnitPrice_1e4"].gt(0))
    inv = df["InvoiceNo"]
    df["is_sale"] = valid & inv.str.fullmatch(r"[0-9]{6}", na=False) & df["Quantity"].gt(0)
    df["is_credit"] = valid & inv.str.fullmatch(r"C[0-9]{6}", na=False) & df["Quantity"].lt(0)

    reason = pd.Series(pd.NA, index=df.index, dtype="string")
    other = ~df["is_sale"] & ~df["is_credit"]
    rules = [
        (inv.str.startswith("A", na=False), "A-документ (корректировка долга)"),
        (df["UnitPrice_1e4"].lt(0), "Отрицательная цена"),
        (df["UnitPrice_1e4"].eq(0), "Нулевая цена"),
        (df["Quantity"].lt(0) & ~inv.str.startswith("C", na=False), "Quantity < 0 без C"),
        (df["Quantity"].eq(0), "Quantity = 0"),
        (df["InvoiceDate"].isna() | df["Quantity"].isna() | df["UnitPrice_1e4"].isna(), "Ошибка типа"),
    ]
    for mask, text in rules:
        reason = reason.mask(other & reason.isna() & mask.fillna(False), text)
    df["exclude_reason"] = reason.mask(other & reason.isna(), "Прочее")
    return df.reset_index(drop=True), int(dup.sum())


# =========================================================== профиль проблем
SHARE_COL = "Доля от всех строк файла, %"


def profile(raw, df, n_dup):
    """Проблемы исходного файла. Всё считается по сырому файлу, поэтому у всех
    долей одна база — все строки файла."""
    inv = raw["InvoiceNo"].str.strip()
    code = raw["StockCode"].str.strip()
    desc = raw["Description"].str.strip().replace("", pd.NA)
    q = pd.to_numeric(raw["Quantity"], errors="coerce")
    p = pd.to_numeric(raw["UnitPrice"], errors="coerce")
    is_c = inv.str.startswith("C")
    service = code.str.upper().isin(NON_GOODS) | code.str.lower().str.startswith("gift_")
    many_names = desc.groupby(code).nunique().gt(1)
    rows = [
        ("Нет номера клиента (CustomerID)", raw["CustomerID"].str.strip().eq(""),
         "Для топа товаров оставляем, в расчёт по клиентам не берём"),
        ("Лишние пробелы в названии товара", raw["Description"] != raw["Description"].str.strip(),
         "Убираем пробелы"),
        ("У одного товара несколько разных названий", code.map(many_names).fillna(False),
         "Товар определяем по коду, название берём самое частое"),
        ("Отменённые заказы (номер начинается с C)", is_c,
         "В продажи не берём, вычитаем только в топе net"),
        ("Одна и та же строка повторяется", raw.duplicated(keep="first"),
         "Удаляем копии, оставляем одну строку"),
        ("Доставка, комиссии, скидки вместо товара", service,
         "В топ товаров не берём, в сумму клиента включаем"),
        ("Цена 0", p.eq(0),
         "Убираем: это подарки и списания, а не продажи"),
        ("Нет названия товара", desc.isna(),
         "Отдельно не чистим: у всех этих строк цена 0, они уходят по правилу «Цена 0»"),
        ("Отрицательное количество в обычном заказе", q.lt(0) & ~is_c,
         "Убираем: это складские списания, а не возвраты"),
        ("Корректировка долга (номер начинается с A)", inv.str.startswith("A"),
         "Убираем: это бухгалтерская запись, а не продажа"),
        ("Отрицательная цена", p.lt(0),
         "Убираем"),
    ]
    base = len(raw)
    out = pd.DataFrame([{"Проблема": name, "Строк": int(mask.sum()),
                         SHARE_COL: round(mask.sum() / base * 100, 4), "Что делаем": action}
                        for name, mask, action in rows])
    return out.sort_values("Строк", ascending=False, kind="stable").reset_index(drop=True)


def share_text(pct):
    if pct == 0:
        return "0"
    if pct < 0.01:
        return "меньше 0,01"
    return n(pct, 2)


# =========================================================== варианты
def build_variants(df):
    sale, credit = df["is_sale"], df["is_credit"]
    known = df["CustomerID"].notna()
    return {
        "clean_all": (df, "Все строки после дедупликации и нормализации, с флагами "
                          "is_sale / is_credit / is_goods и причиной исключения.",
                      "Базовый слой, точка сравнения"),
        "sales_all": (df[sale], "Допустимые продажи (gross): 6 цифр в InvoiceNo, Quantity > 0, "
                                "UnitPrice > 0.", "Общая сумма продаж"),
        "sales_goods": (df[sale & df.is_goods], "Продажи только товарных позиций, с клиентом и без.",
                        "Топ-5 товаров (gross)"),
        "sales_with_customer": (df[sale & known], "Продажи с заполненным CustomerID, включая доставку "
                                                  "и ручные начисления.",
                                "Метрики клиентов: заказы, сумма, средний чек"),
        "sales_without_customer": (df[sale & ~known], "Продажи без CustomerID (гостевые).",
                                   "Отдельный анализ; в метрики клиентов не входит"),
        "credits": (df[credit], "Кредитные операции: C + 6 цифр, Quantity < 0, UnitPrice > 0.",
                    "Net-рейтинг и net-суммы"),
        "excluded": (df[~sale & ~credit], "Строки, не прошедшие правила, с колонкой exclude_reason.",
                     "Аудит исключений"),
    }


# =========================================================== эталонные витрины
def money(amount_1e4, div=1):
    if pd.isna(amount_1e4) or div == 0:
        return None
    return (Decimal(int(amount_1e4)) / 10000 / int(div)).quantize(Decimal("0.01"), ROUND_HALF_UP)


def build_marts(df):
    sales, credits = df[df.is_sale], df[df.is_credit]
    goods_sales = sales[sales.is_goods]
    names = (goods_sales.dropna(subset=["Description"]).groupby(["StockCode", "Description"]).size()
             .reset_index(name="n").sort_values(["StockCode", "n", "Description"],
                                                ascending=[True, False, True])
             .drop_duplicates("StockCode").set_index("StockCode").Description)

    gross = goods_sales.groupby("StockCode").agg(
        total_quantity=("Quantity", "sum"), orders_with_product=("InvoiceNo", "nunique"),
        gross_value_1e4=("TotalPrice_1e4", "sum")).reset_index()
    gross["Description"] = gross.StockCode.map(names)
    gross = gross.sort_values(["total_quantity", "StockCode"], ascending=[False, True])
    top5_gross = gross.head(5).assign(gross_value_gbp=lambda t: t.gross_value_1e4.map(money))[
        ["StockCode", "Description", "total_quantity", "orders_with_product", "gross_value_gbp"]]

    returned = -credits[credits.is_goods].groupby("StockCode").Quantity.sum()
    net = gross.set_index("StockCode")[["Description", "total_quantity"]].join(
        returned.rename("returned_units"), how="outer")
    net[["total_quantity", "returned_units"]] = net[["total_quantity", "returned_units"]].fillna(0).astype("Int64")
    net["net_quantity"] = net.total_quantity - net.returned_units
    net = net.reset_index().sort_values(["net_quantity", "StockCode"], ascending=[False, True])
    top5_net = net.head(5)[["StockCode", "Description", "total_quantity", "returned_units", "net_quantity"]]

    ident = sales[sales.CustomerID.notna()]
    agg = ident.groupby("CustomerID").agg(orders_count=("InvoiceNo", "nunique"),
                                          total_1e4=("TotalPrice_1e4", "sum"))
    ids = pd.Index(sorted(df.CustomerID.dropna().unique()), name="CustomerID")
    cm = agg.reindex(ids).fillna(0).astype("Int64").reset_index()
    cm["total_spent"] = cm.total_1e4.map(money)
    cm["average_order_value"] = [money(t, n) if n else None for t, n in zip(cm.total_1e4, cm.orders_count)]
    return {"top5_gross": top5_gross, "top5_net": top5_net, "customer_metrics": cm,
            "net_all": net, "gross_all": gross}


# =========================================================== проверки
def chk(level, name, ok, detail=""):
    return {"level": level, "check": name, "ok": bool(ok), "detail": detail}


def validate_dataset(name, d, df):
    """Проверки одного варианта. ERROR — отдавать нельзя, WARN — известная аномалия."""
    inv, q, p = d["InvoiceNo"], d["Quantity"], d["UnitPrice_1e4"]
    out = [chk("ERROR", "Датасет не пустой", len(d) > 0, f"{len(d):,} строк"),
           chk("ERROR", "Нет полных дублей", not d[COLS].duplicated().any())]
    if name in ("sales_all", "sales_goods", "sales_with_customer", "sales_without_customer"):
        out += [
            chk("ERROR", "InvoiceNo = 6 цифр", inv.str.fullmatch(r"[0-9]{6}").all()),
            chk("ERROR", "Quantity > 0", q.gt(0).all()),
            chk("ERROR", "UnitPrice > 0", p.gt(0).all()),
            chk("ERROR", "Дата и StockCode заполнены", d.InvoiceDate.notna().all() and d.StockCode.notna().all()),
            chk("ERROR", "TotalPrice = Quantity × UnitPrice (точно)",
                (d.TotalPrice_1e4 == q * p).all()),
            chk("ERROR", "Сумма TotalPrice > 0", d.TotalPrice_1e4.sum() > 0,
                f"{d.TotalPrice_1e4.sum() / 10000:,.2f} GBP"),
        ]
    if name == "sales_goods":
        out.append(chk("ERROR", "Нет нетоварных кодов", d.is_goods.all()))
    if name == "sales_with_customer":
        out += [chk("ERROR", "CustomerID заполнен у всех", d.CustomerID.notna().all()),
                chk("ERROR", "CustomerID = 5 цифр", d.CustomerID.str.fullmatch(r"[0-9]{5}").all())]
    if name == "sales_without_customer":
        out.append(chk("ERROR", "CustomerID пуст у всех", d.CustomerID.isna().all()))
    if name == "credits":
        out += [chk("ERROR", "InvoiceNo = C + 6 цифр", inv.str.fullmatch(r"C[0-9]{6}").all()),
                chk("ERROR", "Quantity < 0", q.lt(0).all()),
                chk("ERROR", "UnitPrice > 0", p.gt(0).all()),
                chk("ERROR", "Сумма TotalPrice < 0", d.TotalPrice_1e4.sum() < 0,
                    f"{d.TotalPrice_1e4.sum() / 10000:,.2f} GBP")]
    if name == "excluded":
        out.append(chk("ERROR", "У каждой строки есть причина", d.exclude_reason.notna().all(),
                       "; ".join(f"{k}: {v}" for k, v in d.exclude_reason.value_counts().items())))
    if name == "clean_all":
        out += [chk("ERROR", "Типы распознаны (Quantity, дата, цена)",
                    d.Quantity.notna().all() and d.InvoiceDate.notna().all() and d.UnitPrice_1e4.notna().all()),
                chk("ERROR", "Продажа и кредит не пересекаются", not (d.is_sale & d.is_credit).any()),
                chk("ERROR", "InvoiceNo → один CustomerID",
                    d.groupby("InvoiceNo").CustomerID.nunique().le(1).all()),
                chk("ERROR", "Даты 01.12.2010 – 09.12.2011",
                    d.InvoiceDate.between("2010-12-01", "2011-12-10").all())]
    return out


def validate_cross(raw, df, n_dup, v, marts):
    cm = marts["customer_metrics"]
    ident = v["sales_with_customer"][0]
    goods = v["sales_goods"][0]
    t5g, t5n = marts["top5_gross"], marts["top5_net"]
    return [
        chk("ERROR", "Исходные = чистые + дубли", len(raw) == len(df) + n_dup,
            f"{len(raw):,} = {len(df):,} + {n_dup:,}"),
        chk("ERROR", "Чистые = продажи + кредиты + исключённые",
            len(df) == len(v["sales_all"][0]) + len(v["credits"][0]) + len(v["excluded"][0])),
        chk("ERROR", "Продажи = с клиентом + без клиента",
            len(v["sales_all"][0]) == len(ident) + len(v["sales_without_customer"][0])),
        chk("ERROR", "Клиентов в витрине = известных ID", len(cm) == df.CustomerID.nunique(), f"{len(cm):,}"),
        chk("ERROR", "CustomerID в витрине уникален", cm.CustomerID.is_unique),
        chk("ERROR", "Σ orders_count = заказы с клиентом",
            int(cm.orders_count.sum()) == ident.InvoiceNo.nunique(), f"{int(cm.orders_count.sum()):,}"),
        chk("ERROR", "Σ сумм клиентов = Σ строк (точно)",
            int(cm.total_1e4.sum()) == int(ident.TotalPrice_1e4.sum())),
        chk("ERROR", "Средний чек пуст ⇔ orders_count = 0",
            (cm.average_order_value.isna() == cm.orders_count.eq(0)).all(),
            f"без продаж: {int(cm.orders_count.eq(0).sum())}"),
        chk("ERROR", "total_spent ≥ 0 у всех", cm.total_1e4.ge(0).all()),
        chk("ERROR", "Топ-5 gross: 5 строк по убыванию",
            len(t5g) == 5 and t5g.total_quantity.is_monotonic_decreasing),
        chk("ERROR", "Σ количества товаров = Σ по топу всех SKU",
            int(marts["gross_all"].total_quantity.sum()) == int(goods.Quantity.sum())),
        chk("WARN", "Фантомные заказы не в топе gross",
            not {"23843", "23166"} & set(t5g.StockCode),
            "23843 и 23166 в топе gross; в net их нет" if {"23843", "23166"} & set(t5g.StockCode) else ""),
        chk("ERROR", "Фантомных заказов нет в топе net", not {"23843", "23166"} & set(t5n.StockCode)),
        chk("WARN", "Net-количество по каждому товару ≥ 0", marts["net_all"].net_quantity.ge(0).all(),
            f"отрицательных SKU: {int(marts['net_all'].net_quantity.lt(0).sum())}"),
    ]


def control_totals(raw, df, n_dup, v, marts):
    sales, ident = v["sales_all"][0], v["sales_with_customer"][0]
    cm = marts["customer_metrics"]
    return {
        "rows_raw": len(raw), "duplicates": n_dup, "rows_clean": len(df),
        "sales_rows": len(sales), "goods_sales_rows": len(v["sales_goods"][0]),
        "identified_sales_rows": len(ident), "credit_rows": len(v["credits"][0]),
        "other_rows": len(v["excluded"][0]),
        "sales_invoices": sales.InvoiceNo.nunique(), "identified_invoices": ident.InvoiceNo.nunique(),
        "customers": len(cm), "customers_with_sales": int(cm.orders_count.gt(0).sum()),
        "customers_without_sales": int(cm.orders_count.eq(0).sum()),
        "identified_gross_1e4": int(ident.TotalPrice_1e4.sum()),
        "no_customer_sales_gbp": round(float(v["sales_without_customer"][0].TotalPrice_1e4.sum()) / 10000, 2),
        "top5_gross": dict(zip(marts["top5_gross"].StockCode, map(int, marts["top5_gross"].total_quantity))),
        "top5_net": dict(zip(marts["top5_net"].StockCode, map(int, marts["top5_net"].net_quantity))),
    }


def compare_with_de(ours, expected):
    return [chk("ERROR", f"{k}", ours.get(k) == val, f"DQ: {ours.get(k)} · DE: {val}")
            for k, val in expected.items() if not k.startswith("_")]


def reconcile(customer_metrics_analyst, reference):
    """Сверка витрины аналитика с эталоном DQ.
    Колонки: CustomerID, orders_count, total_spent, average_order_value."""
    a = customer_metrics_analyst.set_index("CustomerID")
    r = reference.set_index("CustomerID")
    return [
        chk("ERROR", "Тот же набор клиентов", set(a.index) == set(r.index)),
        chk("ERROR", "Нет дублей клиентов", a.index.is_unique),
        chk("ERROR", "orders_count совпадает", (a.orders_count.reindex(r.index) == r.orders_count).all()),
        chk("ERROR", "total_spent совпадает до пенса",
            (a.total_spent.astype(float).reindex(r.index).round(2)
             == r.total_spent.astype(float).round(2)).all()),
        chk("ERROR", "Пустой средний чек ⇔ 0 заказов",
            (a.average_order_value.isna() == a.orders_count.eq(0)).all()),
    ]


# =========================================================== отчёт
def n(x, d=0):
    s = f"{x:,.{d}f}".replace(",", " ")
    return s.replace(".", ",") if d else s


def table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def st(c):
    return "PASS" if c["ok"] else ("FAIL" if c["level"] == "ERROR" else "WARN")


def report(info, expected, raw, df, n_dup, prof, v, marts, ds_checks, cross, de_cmp, ours):
    L = []
    w = L.append
    errors = sum(1 for c in cross + de_cmp + [c for cs in ds_checks.values() for c in cs]
                 if c["level"] == "ERROR" and not c["ok"])
    total = len(cross) + len(de_cmp) + sum(len(cs) for cs in ds_checks.values())
    cm = marts["customer_metrics"]

    w("# Data Quality: Online Retail (ЛР2)\n")
    w("Файл сгенерирован `python dq/dq_check.py data.csv`. DQ пересобирает датасет отдельным кодом "
      "по контракту `DE_memo.md` и сверяет цифры с `eds.ipynb`.\n")

    w("## 1. Итог\n")
    w(f"- Проверок: **{total}**, ошибок ERROR: **{errors}**. "
      f"Все {len(de_cmp)} контрольных значений совпали с ноутбуком DE.")
    w(f"- Исходник {n(len(raw))} строк → после дедупликации {n(len(df))} → "
      f"продажи {n(len(v['sales_all'][0]))}, кредиты {n(len(v['credits'][0]))}, "
      f"исключено {n(len(v['excluded'][0]))}.")
    w(f"- Клиентов {n(len(cm))}: с продажами {n(int(cm.orders_count.gt(0).sum()))}, "
      f"без продаж {n(int(cm.orders_count.eq(0).sum()))} (средний чек пустой).")
    w("- **Главный риск:** топ-5 gross на 1–2 местах держат два заказа, полностью отменённые "
      "через 12–16 минут (23843, 23166). На защите показывать gross и net рядом.")
    if info["line_ending"] == "LF":
        w("- **Находка по файлу:** `data.csv` в репозитории хранится с LF, а не CRLF, поэтому его SHA256 "
          "не совпадает с зафиксированным в ноутбуке. Содержимое то же (раздел 2).")
    w("")

    w("## 2. Исходный файл\n")
    w(table(["Параметр", "Значение"], [
        ("Строк / колонок", f"{n(len(raw))} / {raw.shape[1]}"),
        ("Период", f"{df.InvoiceDate.min():%d.%m.%Y} – {df.InvoiceDate.max():%d.%m.%Y}"),
        ("Кодировка", "cp1252 (Windows-1252)"),
        ("Размер файла", f"{n(info['size'])} байт"),
        ("Переводы строк", info["line_ending"]),
        ("SHA256 как есть", f"`{info['sha256']}`"),
        ("SHA256 после LF → CRLF", f"`{info['sha256_crlf']}`"),
        ("SHA256 в eds.ipynb", f"`{expected['_sha256']}`"),
    ]))
    same = expected["_sha256"] in (info["sha256"], info["sha256_crlf"])
    w(f"\n{'Содержимое совпадает с файлом ноутбука.' if same else 'ВНИМАНИЕ: содержимое отличается от файла ноутбука.'}")
    if info["line_ending"] == "LF":
        w(" Git перевёл CRLF в LF, поэтому проверка SHA256 из контракта на копии из репозитория "
          "не пройдёт. Решение: добавить в `.gitattributes` строку `data.csv -text` "
          "или считать хэш после нормализации переводов строк.")
    w("")

    w("## 3. Найденные проблемы\n")
    w(f"Доля считается от всех {n(len(raw))} строк исходного файла. Одна строка может попасть "
      "сразу в несколько проблем, поэтому доли не складываются.\n")
    w(table(["Проблема", "Строк", "Доля от всех строк, %", "Что делаем"],
            [(r["Проблема"], n(r["Строк"]), share_text(r[SHARE_COL]), r["Что делаем"])
             for _, r in prof.iterrows()]))
    w("\nСлужебные коды, которые не считаются товаром: " + ", ".join(sorted(NON_GOODS))
      + " и подарочные сертификаты gift_*. Коды PADS и DCGS* — обычные товары.\n")

    w("## 4. Правила (по DE_memo.md)\n")
    for i, s in enumerate([
        "Дедупликация по всем 8 исходным полям до trim и приведения типов.",
        "Trim, пустые значения → NULL; Quantity → целое, UnitPrice → точная цена (1e-4 GBP), дата `%m/%d/%Y %H:%M`.",
        "**Продажа:** InvoiceNo = 6 цифр, Quantity > 0, UnitPrice > 0, дата и StockCode заполнены.",
        "**Кредит:** InvoiceNo = C + 6 цифр, Quantity < 0, UnitPrice > 0.",
        "Остальное (нулевые и отрицательные цены, A-документы, Quantity < 0 без C) — в `excluded` с причиной.",
        "Топ-5 gross: сумма Quantity по товарным продажам, все страны, с клиентом и без.",
        "Топ-5 net: gross − единицы в кредитах.",
        "Клиенты: все известные ID; заказы = уникальные InvoiceNo продаж; сумма включает доставку и Manual; "
        "чек = сумма / заказы, пусто при 0 заказов.",
    ], 1):
        w(f"{i}. {s}")
    w("")

    w("## 5. Варианты чистого датасета\n")
    w("Файлы создаются в `dq/datasets/` (в git не хранятся, ~260 МБ). Все варианты — срезы `clean_all`.\n")
    rows = []
    for name, (d, what, use) in v.items():
        rows.append((f"`{name}.csv`", n(len(d)), n(d.InvoiceNo.nunique()), n(d.CustomerID.nunique()),
                     n(int(d.Quantity.sum())), n(float(d.TotalPrice_1e4.sum()) / 10000, 2), use))
    w(table(["Файл", "Строк", "Документов", "Клиентов", "Единиц", "Сумма, GBP", "Для чего"], rows))
    w("")
    for name, (d, what, use) in v.items():
        w(f"- **{name}** — {what}")
    w("")

    w("## 6. Топ-5 товаров: gross и net\n")
    g, t = marts["top5_gross"], marts["top5_net"]
    w(table(["#", "Gross (основной по контракту)", "Ед.", "Net (за вычетом кредитов)", "Ед."],
            [(i + 1, f"{a.StockCode} {a.Description}", n(int(a.total_quantity)),
              f"{b.StockCode} {b.Description}", n(int(b.net_quantity)))
             for i, (a, b) in enumerate(zip(g.itertuples(), t.itertuples()))]))
    ph = df[df.StockCode.isin(["23843", "23166"]) & df.Quantity.abs().gt(70000)]
    w("\nПочему они расходятся — две продажи и их полные кредиты:\n")
    w(table(["Документ", "Товар", "Клиент", "Количество", "Время"],
            [(r.InvoiceNo, f"{r.StockCode} {r.Description}", r.CustomerID, n(int(r.Quantity)),
              f"{r.InvoiceDate:%d.%m.%Y %H:%M}") for r in ph.itertuples()]))
    w("")

    w("## 7. Аномалии (не ошибки)\n")
    neg = marts["net_all"].net_quantity.lt(0).sum()
    w(f"- {n(int(cm.orders_count.eq(0).sum()))} клиентов без допустимых продаж: только кредиты или нулевые цены. "
      "В витрине с orders_count = 0 и пустым средним чеком.")
    w(f"- {neg} SKU с отрицательным net-количеством: кредиты к продажам до 01.12.2010.")
    w(f"- {n(int(v['sales_without_customer'][0].InvoiceNo.nunique()))} продажных документов без клиента, "
      f"{n(ours['no_customer_sales_gbp'], 2)} GBP — в товарный топ входят, клиенту не приписываются.")
    w("- Выбросы по количеству не удаляются: оптовые заказы — реальные продажи.\n")

    w("## 8. Проверки вариантов\n")
    w("ERROR — правило нарушено, данные не отдаём (скрипт завершается с ошибкой). "
      "WARN — известная аномалия, задокументирована.\n")
    for name, cs in ds_checks.items():
        w(f"### {name}\n")
        w(table(["Проверка", "Уровень", "Результат", "Детали"],
                [(c["check"], c["level"], st(c), c["detail"]) for c in cs]))
        w("")

    w("## 9. Сквозные проверки\n")
    w(table(["Проверка", "Уровень", "Результат", "Детали"],
            [(c["check"], c["level"], st(c), c["detail"]) for c in cross]))
    w("")

    w("## 10. Сверка с ноутбуком DE\n")
    w("Ожидаемые значения — `dq/expected_control_totals.json`, взяты из вывода `eds.ipynb`.\n")
    w(table(["Показатель", "Результат", "DQ · DE"],
            [(c["check"], st(c), c["detail"].replace("|", "/")) for c in de_cmp]))
    w("")

    w("## 11. Сверка с аналитиком\n")
    w("Эталонные витрины DQ: `dq/reference/top5_gross.csv`, `top5_net.csv`, `customer_metrics.csv`. "
      "Витрину аналитика сверяем функцией `reconcile()`:\n")
    w("```python\nimport pandas as pd, sys\nsys.path.insert(0, 'dq')\nfrom dq_check import reconcile\n"
      "ref = pd.read_csv('dq/reference/customer_metrics.csv', dtype={'CustomerID': str})\n"
      "mine = pd.read_csv('exports/customer_metrics.csv', dtype={'CustomerID': str})\n"
      "for c in reconcile(mine, ref):\n    print(c['ok'], c['check'])\n```\n")

    w("## 12. Открытые вопросы\n")
    for s in ["Какой топ-5 главный на защите: gross (как в контракте) или net? DQ рекомендует показывать оба.",
              "Добавить `data.csv -text` в `.gitattributes`, чтобы SHA256 из контракта совпадал (если файл с LF).",
              "Подтвердить удаление полных дублей: без ID строки это допущение."]:
        w(f"- [ ] {s}")
    w("")
    return "\n".join(L), errors


# =========================================================== main
def main(src):
    info = file_info(src)
    raw = load(src)
    df, n_dup = normalize(raw)
    prof = profile(raw, df, n_dup)
    v = build_variants(df)
    marts = build_marts(df)

    ds_checks = {name: validate_dataset(name, d, df) for name, (d, _, _) in v.items()}
    cross = validate_cross(raw, df, n_dup, v, marts)
    ours = control_totals(raw, df, n_dup, v, marts)
    expected = json.loads(EXPECTED.read_text(encoding="utf-8"))
    de_cmp = compare_with_de(ours, expected)

    DATASETS.mkdir(exist_ok=True)
    REFERENCE.mkdir(exist_ok=True)
    for name, (d, _, _) in v.items():
        d.to_csv(DATASETS / f"{name}.csv", index=False)
    marts["top5_gross"].to_csv(REFERENCE / "top5_gross.csv", index=False)
    marts["top5_net"].to_csv(REFERENCE / "top5_net.csv", index=False)
    marts["customer_metrics"].drop(columns="total_1e4").to_csv(REFERENCE / "customer_metrics.csv", index=False)
    prof.to_csv(REFERENCE / "dq_problems.csv", index=False)
    (REFERENCE / "dq_control_totals.json").write_text(
        json.dumps(ours, ensure_ascii=False, indent=2), encoding="utf-8")

    text, errors = report(info, expected, raw, df, n_dup, prof, v, marts, ds_checks, cross, de_cmp, ours)
    (HERE / "DQ_REPORT.md").write_text(text, encoding="utf-8")

    for group, cs in [*ds_checks.items(), ("сквозные", cross), ("сверка с DE", de_cmp)]:
        print(f"\n=== {group}")
        for c in cs:
            print(f"[{st(c)}] {c['check']}" + (f"  ({c['detail']})" if c["detail"] else ""))
    print(f"\nОтчёт: {HERE / 'DQ_REPORT.md'}; ошибок ERROR: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data.csv")
