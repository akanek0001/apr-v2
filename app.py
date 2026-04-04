from __future__ import annotations

# =========================================================
# IMPORT
# =========================================================
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Any, Dict, List, Optional, Set, Tuple
import json
import re

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageEnhance, ImageFilter, ImageOps, ImageDraw

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# =========================================================
# CONFIG
# =========================================================
class AppConfig:
    APP_TITLE = "APR資産運用管理システム"
    APP_ICON = "🏦"
    PAGE_LAYOUT = "wide"
    JST = timezone(timedelta(hours=9), "JST")

    STATUS = {"ON": "🟢運用中", "OFF": "🔴停止"}
    RANK = {"MASTER": "Master", "ELITE": "Elite"}
    FACTOR = {"MASTER": 0.67, "ELITE": 0.60}
    RANK_LABEL = "👑Master=67% / 🥈Elite=60%"

    PROJECT = {"PERSONAL": "PERSONAL"}
    COMPOUND = {"DAILY": "daily", "MONTHLY": "monthly", "NONE": "none"}
    COMPOUND_LABEL = {"daily": "日次複利", "monthly": "月次複利", "none": "単利"}

    TYPE = {"APR": "APR", "LINE": "LINE", "DEPOSIT": "Deposit", "WITHDRAW": "Withdraw"}
    SOURCE = {"APP": "app"}

    SHEET = {
        "SETTINGS": "Settings",
        "MEMBERS": "Members",
        "LEDGER": "Ledger",
        "LINEUSERS": "LineUsers",
        "APR_SUMMARY": "APR_Summary",
        "SMARTVAULT_HISTORY": "SmartVault_History",
        "USDC_HISTORY": "USDC_History",
    }

    HEADERS = {
        "SETTINGS": [
            "Project_Name",
            "Net_Factor",
            "IsCompound",
            "Compound_Timing",
            "Crop_Left_Ratio_PC",
            "Crop_Top_Ratio_PC",
            "Crop_Right_Ratio_PC",
            "Crop_Bottom_Ratio_PC",
            "Crop_Left_Ratio_Mobile",
            "Crop_Top_Ratio_Mobile",
            "Crop_Right_Ratio_Mobile",
            "Crop_Bottom_Ratio_Mobile",
            # SmartVault Mobile 3-zone OCR boxes
            "SV_Liq_Left", "SV_Liq_Top", "SV_Liq_Right", "SV_Liq_Bottom",
            "SV_Profit_Left", "SV_Profit_Top", "SV_Profit_Right", "SV_Profit_Bottom",
            "SV_APR_Left", "SV_APR_Top", "SV_APR_Right", "SV_APR_Bottom",
            # PC 3-zone OCR boxes (liquidity & profit; APR uses existing Crop_* columns)
            "PC_Liq_Left", "PC_Liq_Top", "PC_Liq_Right", "PC_Liq_Bottom",
            "PC_Profit_Left", "PC_Profit_Top", "PC_Profit_Right", "PC_Profit_Bottom",
            "UpdatedAt_JST",
            "Active",
        ],
        "MEMBERS": [
            "Project_Name",
            "PersonName",
            "Principal",
            "Line_User_ID",
            "LINE_DisplayName",
            "Rank",
            "IsActive",
            "CreatedAt_JST",
            
            "UpdatedAt_JST",
        ],
        "LEDGER": [
            "Datetime_JST",
            "Project_Name",
            "PersonName",
            "Type",
            "Amount",
            "Note",
            "Evidence_URL",
            "Line_User_ID",
            "LINE_DisplayName",
            "Source",
        ],
        "LINEUSERS": ["Date", "Time", "Type", "Line_User_ID", "Line_User"],
        "APR_SUMMARY": ["Date_JST", "PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"],
        "SMARTVAULT_HISTORY": [
            "Datetime_JST",
            "Project_Name",
            "Liquidity",
            "Yesterday_Profit",
            "APR",
            "Source_Mode",
            "OCR_Liquidity",
            "OCR_Yesterday_Profit",
            "OCR_APR",
            "Evidence_URL",
            "Admin_Name",
            "Admin_Namespace",
            "Note",
            "Device_Type",   # "pc" or "mobile" — step3 デバイス判定
        ],
        "USDC_HISTORY": [
            "Unique_Key",
            "Date_Label",
            "Time_Label",
            "Type_Label",
            "Amount_USD",
            "Token_Amount",
            "Token_Symbol",
            "Source_Image",
            "Source_Project",
            "OCR_Raw_Text",
            "CreatedAt_JST",
        ],
    }

    PAGE = {
        "DASHBOARD": "📊 ダッシュボード",
        "APR": "📈 APR",
        "CASH": "💸 入金/出金",
        "ADMIN": "⚙️ 管理",
        "HELP": "❓ ヘルプ",
    }

    SESSION_KEYS = {
        "SETTINGS": "settings_df",
        "MEMBERS": "members_df",
        "LEDGER": "ledger_df",
        "LINEUSERS": "line_users_df",
        "APR_SUMMARY": "apr_summary_df",
    }

    APR_LINE_NOTE_KEYWORD = "APR:"

    OCR_DEFAULTS_PC = {
        "Crop_Left_Ratio_PC": 0.70,
        "Crop_Top_Ratio_PC": 0.20,
        "Crop_Right_Ratio_PC": 0.90,
        "Crop_Bottom_Ratio_PC": 0.285,
    }

    OCR_DEFAULTS_MOBILE = {
        "Crop_Left_Ratio_Mobile": 0.68,
        "Crop_Top_Ratio_Mobile": 0.23,
        "Crop_Right_Ratio_Mobile": 0.92,
        "Crop_Bottom_Ratio_Mobile": 0.355,
    }

    SMARTVAULT_BOXES_MOBILE = {
        "TOTAL_LIQUIDITY": {"left": 0.05, "top": 0.25, "right": 0.40, "bottom": 0.34},
        "YESTERDAY_PROFIT": {"left": 0.41, "top": 0.25, "right": 0.69, "bottom": 0.34},
        "APR": {"left": 0.70, "top": 0.25, "right": 0.93, "bottom": 0.34},
    }

    # Default values for SmartVault Mobile configurable boxes (mirrors SMARTVAULT_BOXES_MOBILE)
    # 上端(Liq)・下端(APR) が読み取れない問題 対応:
    #   - Top: 0.11、Bottom: 0.30（全ゾーン）
    #   - 左端ゾーン(Liq)は左端まで拡張、右端ゾーン(APR)は右端まで拡張
    SV_BOX_DEFAULTS: Dict[str, float] = {
        "SV_Liq_Left": 0.02,  "SV_Liq_Top": 0.11,  "SV_Liq_Right": 0.43,  "SV_Liq_Bottom": 0.30,
        "SV_Profit_Left": 0.40, "SV_Profit_Top": 0.11, "SV_Profit_Right": 0.70, "SV_Profit_Bottom": 0.30,
        "SV_APR_Left": 0.67,  "SV_APR_Top": 0.11,  "SV_APR_Right": 0.99,  "SV_APR_Bottom": 0.30,
    }

    # Default values for PC 3-zone boxes
    # 操作履歴パネル（画面右端 x≈0.79〜1.0）から読み取る構成:
    #   PC_Liq   : 右パネル上部 「提供した流動性の合計」$XX,XXX → y=0.19〜0.30
    #   PC_Profit: 右パネル中部〜下部 手数料を回収エントリ（1日前・当日）→ y=0.48〜1.0 末尾の $ 値を採用
    PC_BOX_DEFAULTS: Dict[str, float] = {
        "PC_Liq_Left": 0.79, "PC_Liq_Top": 0.19, "PC_Liq_Right": 1.0, "PC_Liq_Bottom": 0.30,
        "PC_Profit_Left": 0.79, "PC_Profit_Top": 0.48, "PC_Profit_Right": 1.0, "PC_Profit_Bottom": 1.0,
    }

    # Auto-expand margin when OCR detects nothing (ratio units)
    OCR_EXPAND_MARGIN: float = 0.04


# =========================================================
# UTILS
# =========================================================
class U:
    @staticmethod
    def now_jst() -> datetime:
        return datetime.now(AppConfig.JST)

    @staticmethod
    def fmt_dt(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def fmt_date(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def fmt_usd(x: float) -> str:
        return f"${x:,.2f}"

    @staticmethod
    def to_f(v: Any) -> float:
        try:
            s = str(v).replace(",", "").replace("$", "").replace("%", "").strip()
            return float(s) if s else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def to_num_series(s: pd.Series, default: float = 0.0) -> pd.Series:
        out = pd.to_numeric(
            s.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip(),
            errors="coerce",
        )
        return out.fillna(default)

    @staticmethod
    def truthy(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "y", "on", "はい", "t")

    @staticmethod
    def truthy_series(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.lower().isin(["1", "true", "yes", "y", "on", "はい", "t"])

    @staticmethod
    def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = out.columns.astype(str).str.replace("\u3000", " ", regex=False).str.strip()
        return out

    @staticmethod
    def extract_sheet_id(value: str) -> str:
        sid = (value or "").strip()
        if "/spreadsheets/d/" in sid:
            try:
                sid = sid.split("/spreadsheets/d/")[1].split("/")[0]
            except Exception:
                pass
        return sid

    @staticmethod
    def normalize_rank(rank: Any) -> str:
        return AppConfig.RANK["ELITE"] if str(rank).strip().lower() == "elite" else AppConfig.RANK["MASTER"]

    @staticmethod
    def rank_factor(rank: Any) -> float:
        return AppConfig.FACTOR["ELITE"] if str(rank).strip().lower() == "elite" else AppConfig.FACTOR["MASTER"]

    @staticmethod
    def bool_to_status(v: Any) -> str:
        return AppConfig.STATUS["ON"] if U.truthy(v) else AppConfig.STATUS["OFF"]

    @staticmethod
    def status_to_bool(v: Any) -> bool:
        return str(v).strip() == AppConfig.STATUS["ON"]

    @staticmethod
    def normalize_compound(v: Any) -> str:
        s = str(v).strip().lower()
        return s if s in AppConfig.COMPOUND.values() else AppConfig.COMPOUND["NONE"]

    @staticmethod
    def compound_label(v: Any) -> str:
        return AppConfig.COMPOUND_LABEL[U.normalize_compound(v)]

    @staticmethod
    def is_line_uid(v: Any) -> bool:
        s = str(v).strip()
        return s.startswith("U") and len(s) >= 10

    @staticmethod
    def sheet_name(base: str, ns: str) -> str:
        ns = str(ns or "").strip()
        return base if not ns or ns == "default" else f"{base}__{ns}"

    @staticmethod
    def insert_person_name(msg_common: str, person_name: str) -> str:
        name_line = f"{person_name} 様"
        lines = msg_common.splitlines()
        if name_line in lines:
            return msg_common
        if lines and lines[0].strip() == "【ご連絡】":
            return "\n".join([lines[0], name_line] + lines[1:])
        return "\n".join([name_line] + lines)

    @staticmethod
    def apr_val(x: str) -> float:
        s = str(x).replace("%", "").replace(",", "").strip()
        if not s:
            return 0.0
        try:
            return float(s)
        except Exception:
            return 0.0

    @staticmethod
    def to_ratio(v: Any, default: float) -> float:
        try:
            x = float(str(v).strip())
            if 0.0 <= x <= 1.0:
                return x
            return default
        except Exception:
            return default

    @staticmethod
    def crop_image_by_ratio(
        file_bytes: bytes,
        left_ratio: float,
        top_ratio: float,
        right_ratio: float,
        bottom_ratio: float,
    ) -> bytes:
        try:
            img = Image.open(BytesIO(file_bytes)).convert("RGB")
            w, h = img.size

            left = max(0, min(int(w * left_ratio), w - 1))
            top = max(0, min(int(h * top_ratio), h - 1))
            right = max(left + 1, min(int(w * right_ratio), w))
            bottom = max(top + 1, min(int(h * bottom_ratio), h))

            cropped = img.crop((left, top, right, bottom))
            buf = BytesIO()
            cropped.save(buf, format="PNG")
            return buf.getvalue()
        except Exception:
            return file_bytes

    @staticmethod
    def is_mobile_tall_image(file_bytes: bytes) -> bool:
        """Return True when the image looks like a mobile (portrait) screenshot.

        Strategy (in order):
        1. Ratio > 1.8  → definitely mobile (very tall)
        2. Ratio > 1.45 → likely mobile only when resolution also suggests a phone
           (width < 900 px  OR  height > 1400 px)
        3. Otherwise → treat as PC / landscape
        """
        try:
            img = Image.open(BytesIO(file_bytes))
            w, h = img.size
            ratio = h / max(w, 1)
            if ratio > 1.8:
                return True
            if ratio > 1.45 and (w < 900 or h > 1400):
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def preprocess_ocr_image(file_bytes: bytes) -> List[bytes]:
        outputs: List[bytes] = []

        try:
            base = Image.open(BytesIO(file_bytes)).convert("L")
            variants: List[Image.Image] = []

            img1 = ImageOps.autocontrast(base)
            img1 = ImageEnhance.Contrast(img1).enhance(3.0)
            img1 = ImageEnhance.Sharpness(img1).enhance(2.5)
            img1 = img1.resize((base.width * 4, base.height * 4))
            variants.append(img1)

            img2 = ImageOps.autocontrast(base)
            img2 = ImageEnhance.Contrast(img2).enhance(3.5)
            img2 = img2.resize((base.width * 5, base.height * 5))
            img2 = img2.point(lambda x: 255 if x > 165 else 0)
            variants.append(img2)

            img3 = ImageOps.autocontrast(base)
            img3 = ImageEnhance.Contrast(img3).enhance(3.2)
            img3 = img3.resize((base.width * 5, base.height * 5))
            img3 = img3.point(lambda x: 255 if x > 145 else 0)
            variants.append(img3)

            img4 = ImageOps.autocontrast(base)
            img4 = img4.filter(ImageFilter.MedianFilter(size=3))
            img4 = ImageEnhance.Contrast(img4).enhance(2.8)
            img4 = ImageEnhance.Sharpness(img4).enhance(3.2)
            img4 = img4.resize((base.width * 4, base.height * 4))
            variants.append(img4)

            for img in variants:
                buf = BytesIO()
                img.save(buf, format="PNG")
                outputs.append(buf.getvalue())

        except Exception:
            return [file_bytes]

        return outputs if outputs else [file_bytes]

    @staticmethod
    def extract_percent_candidates(text: str) -> List[float]:
        if not text:
            return []

        norm = str(text)

        replace_map = {
            "％": "%",
            "O": "0",
            "o": "0",
            "Q": "0",
            "D": "0",
            "I": "1",
            "l": "1",
            "|": "1",
            "S": "5",
            "s": "5",
            ",": ".",
        }
        for k, v in replace_map.items():
            norm = norm.replace(k, v)

        norm = re.sub(r"[ \t\u3000]+", " ", norm)

        patterns = [
            r"(?i)apr\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%",
            r"(?i)apr\s*[:：]?\s*(\d+(?:\.\d+)?)",
            r"(?i)apy\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%",
            r"(?i)rate\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%",
            r"(\d+(?:\.\d+)?)\s*%",
            r"(\d{1,3}\.\d{1,4})",
        ]

        vals: List[float] = []
        seen = set()

        for pat in patterns:
            for v in re.findall(pat, norm):
                try:
                    f = float(v)
                    if 0 <= f <= 300:
                        key = round(f, 6)
                        if key not in seen:
                            seen.add(key)
                            vals.append(f)
                except Exception:
                    pass

        def score(x: float) -> tuple:
            if 1 <= x <= 80:
                return (0, abs(x - 40))
            if 80 < x <= 150:
                return (1, abs(x - 100))
            return (2, x)

        return sorted(vals, key=score)

    @staticmethod
    def extract_usd_candidates(text: str) -> List[float]:
        if not text:
            return []

        norm = str(text)

        replace_map = {
            "＄": "$",
            "，": ",",
            "。": ".",
            "O": "0",
            "o": "0",
            "Q": "0",
            "D": "0",
            "I": "1",
            "l": "1",
            "|": "1",
            "S": "5",
            "s": "5",
        }
        for k, v in replace_map.items():
            norm = norm.replace(k, v)

        norm = re.sub(r"[ \t\u3000]+", " ", norm)

        patterns = [
            r"\$?\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?)",
            r"\$?\s*(\d+\.\d+)",
        ]

        vals: List[float] = []
        seen = set()

        for pat in patterns:
            for v in re.findall(pat, norm):
                try:
                    f = float(str(v).replace(",", ""))
                    if 0 <= f <= 1000000000:
                        key = round(f, 6)
                        if key not in seen:
                            seen.add(key)
                            vals.append(f)
                except Exception:
                    pass

        return vals

    @staticmethod
    def pick_total_liquidity(vals: List[float]) -> Optional[float]:
        if not vals:
            return None
        positives = [float(v) for v in vals if float(v) > 0]
        if not positives:
            return None
        return max(positives)

    @staticmethod
    def pick_yesterday_profit(vals: List[float]) -> Optional[float]:
        if not vals:
            return None

        candidates = [float(v) for v in vals if float(v) >= 0]
        if not candidates:
            return None

        small_first = [v for v in candidates if v <= 1000000]
        if small_first:
            return sorted(small_first)[0] if len(small_first) == 1 else min(small_first, key=lambda x: len(str(int(x))))
        return min(candidates)

    @staticmethod
    def maybe_invert_dark(file_bytes: bytes, threshold: int = 128) -> bytes:
        """ダークモード画像（平均輝度が threshold 未満）を明暗反転して返す。

        Coinbase / スマートウォレットのダークUI は暗い背景に明るい文字のため、
        OCR.space が "$" → "±" 等に誤読する。反転することで白背景+黒文字になり精度が上がる。
        threshold=128: 平均輝度0-255 の中間値。ダークモード画像は通常 80-110 程度のため
        100 だと境界で反転されない場合がある → 128 に引き上げて確実に反転。
        輝度計算は 64×64 に縮小してから行うことで高速化（全ピクセル展開を回避）。
        JPEG保存（quality=90）でファイルサイズを抑え API 送信遅延を防ぐ。
        """
        try:
            img = Image.open(BytesIO(file_bytes)).convert("L")
            # 64×64 に縮小して輝度計算（フルサイズ展開を避ける）
            small = img.resize((64, 64), Image.NEAREST)
            pixels = list(small.getdata())  # 4096 ピクセルのみ
            mean_brightness = sum(pixels) / 4096
            if mean_brightness < threshold:
                inverted = ImageOps.invert(img).convert("RGB")
                buf = BytesIO()
                inverted.save(buf, format="JPEG", quality=90)
                return buf.getvalue()
        except Exception:
            pass
        return file_bytes

    @staticmethod
    def pick_last_fee_amount(vals: List[float], min_v: float = 1.0, max_v: float = 500_000.0) -> Optional[float]:
        """操作履歴パネル OCR 用: 手数料を回収エントリの最新（テキスト末尾）$ 値を返す。

        OCR テキストは上→下の順で読まれるため、範囲内の最後の値が
        最新の「手数料を回収」エントリの金額に対応する。
        """
        in_range = [float(v) for v in vals if min_v <= float(v) <= max_v]
        return in_range[-1] if in_range else None

    @staticmethod
    def pick_prev_fee_amount(vals: List[float], min_v: float = 1.0, max_v: float = 500_000.0) -> Optional[float]:
        """操作履歴パネル OCR 用: 手数料を回収エントリの1日前（末尾から2番目）$ 値を返す。

        エントリが1件のみの場合はその値を返す。
        """
        in_range = [float(v) for v in vals if min_v <= float(v) <= max_v]
        if len(in_range) >= 2:
            return in_range[-2]
        return in_range[-1] if in_range else None

    @staticmethod
    def sum_fee_amounts(vals: List[float], min_v: float = 1.0, max_v: float = 500_000.0) -> float:
        """操作履歴パネル OCR 用: 範囲内の全手数料を回収エントリの合計額を返す。"""
        return sum(float(v) for v in vals if min_v <= float(v) <= max_v)

    @staticmethod
    def extract_history_datetime(text: str) -> Optional[str]:
        """操作履歴パネルの OCR テキストから最新の日時文字列を抽出する。

        対応フォーマット:
          2026/04/03 10:27  →  "2026/04/03 10:27"
          2026.04.03 10:27  →  "2026/04/03 10:27"
          2026-04-03 10:27  →  "2026/04/03 10:27"
        テキスト内に複数ある場合は末尾（最新）を返す。
        """
        if not text:
            return None
        pattern = r"(\d{4})[\/\.\-](\d{1,2})[\/\.\-](\d{1,2})\s+(\d{1,2}:\d{2})"
        matches = re.findall(pattern, text)
        if not matches:
            return None
        y, mo, d, hm = matches[-1]  # 末尾 = 最新エントリ
        return f"{y}/{int(mo):02d}/{int(d):02d} {hm}"

    @staticmethod
    def extract_transaction_rows(text: str) -> List[Dict[str, Any]]:
        """Extract (datetime, date_str, time_str, amount) rows from a USDC transaction
        history OCR text.

        Expected OCR line format (Japanese wallet app):
          "3月 29 at 10:44 am"  followed somewhere by  "$28.19"

        「月」は engine=1 OCR で "B" 等に文字化けすることがある。
        また画像レイアウト上、全日付が先に並び全金額が後にまとまる場合がある
        （カラム読み）ため、その場合は位置ベース（i番目日付↔i番目金額）でマッチする。
        """
        if not text:
            return []

        norm = re.sub(r"[ \t\u3000]+", " ", text)
        # ダークモード OCR 誤読の正規化: "±" → "$"、よくある数字化けも補正
        norm = norm.replace("±", "$").replace("£", "$").replace("§", "$")

        # Pattern: <月数>[月 or OCR誤読文字 or 省略]\s*<日> at <HH>:<MM> <am|pm>
        # \s* を月数字の後にも追加: "4 h 3 at" (月→" h "のように前後スペース付き化け) に対応
        # [^\d\s]? — 1文字の非数字・非スペース（月・B・h など何でも）を許容
        date_pat = re.compile(
            r"(\d{1,2})\s*[^\d\s]?\s*(\d{1,2})\s+at\s+(\d{1,2}:\d{2})\s*(am|pm)",
            re.IGNORECASE,
        )
        amount_pat = re.compile(r"\$\s*(\d[\d,]*(?:\.\d+)?)")

        date_matches = list(date_pat.finditer(norm))
        all_amount_matches = list(amount_pat.finditer(norm))

        if not date_matches:
            return []

        # ── チャンクベースで金額を収集 ──
        chunk_amounts: List[Optional[str]] = []
        for i, dm in enumerate(date_matches):
            start = dm.start()
            end = date_matches[i + 1].start() if i + 1 < len(date_matches) else len(norm)
            chunk = norm[start:end]
            found = amount_pat.findall(chunk)
            chunk_amounts.append(found[0] if found else None)

        # ── カラムレイアウト検出：大半のチャンクに金額がなく全金額が末尾にまとまる場合 ──
        # 日付数より金額数が少ない場合（例: 最後の日付が画像下端で切れている）も
        # zip の自然な停止に任せて位置合わせを行う（件数一致チェックを除去）
        n_found = sum(1 for a in chunk_amounts if a is not None)
        use_positional = (n_found <= 1 and len(all_amount_matches) >= 1)
        if use_positional:
            # i番目の日付 → i番目の金額（カラム順一致）。金額が足りない分は None
            pos_amounts = [m.group(1) for m in all_amount_matches]
            final_amounts: List[Optional[str]] = (
                pos_amounts[: len(date_matches)]
                + [None] * max(0, len(date_matches) - len(pos_amounts))
            )
        else:
            final_amounts = chunk_amounts

        rows: List[Dict[str, Any]] = []
        for i, dm in enumerate(date_matches):
            month = int(dm.group(1))
            day = int(dm.group(2))
            time_str = dm.group(3)
            ampm = dm.group(4).lower()

            hour, minute = (int(p) for p in time_str.split(":"))
            if ampm == "pm" and hour != 12:
                hour += 12
            elif ampm == "am" and hour == 12:
                hour = 0

            try:
                year = U.now_jst().year
                dt = datetime(year, month, day, hour, minute, tzinfo=AppConfig.JST)
            except ValueError:
                dt = None

            amt_str = final_amounts[i]
            amount: Optional[float] = None
            if amt_str:
                try:
                    amount = float(str(amt_str).replace(",", ""))
                except ValueError:
                    pass

            rows.append(
                {
                    "datetime": dt,
                    "date_str": f"{month}月{day}日",
                    "time_str": f"{time_str} {ampm}",
                    "datetime_jst": U.fmt_dt(dt) if dt else "",
                    "amount": amount,
                }
            )

        return rows

    @staticmethod
    def draw_ocr_boxes(file_bytes: bytes, boxes: Dict[str, Dict[str, float]]) -> bytes:
        try:
            img = Image.open(BytesIO(file_bytes)).convert("RGB")
            draw = ImageDraw.Draw(img)
            w, h = img.size

            for label, box in boxes.items():
                left = int(w * box["left"])
                top = int(h * box["top"])
                right = int(w * box["right"])
                bottom = int(h * box["bottom"])

                draw.rectangle((left, top, right, bottom), outline="red", width=4)
                draw.text((left, max(0, top - 20)), label, fill="red")

            buf = BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()
        except Exception:
            return file_bytes

    @staticmethod
    def detect_source_mode(
        final_liquidity: float,
        final_profit: float,
        final_apr: float,
        ocr_liquidity: Optional[float],
        ocr_profit: Optional[float],
        ocr_apr: Optional[float],
    ) -> str:
        has_ocr = any(v is not None for v in [ocr_liquidity, ocr_profit, ocr_apr])
        if not has_ocr:
            return "manual"

        def same(a: Optional[float], b: float) -> bool:
            if a is None:
                return False
            return abs(float(a) - float(b)) < 1e-9

        if same(ocr_liquidity, final_liquidity) and same(ocr_profit, final_profit) and same(ocr_apr, final_apr):
            return "ocr"
        return "ocr+manual"

    @staticmethod
    def calc_combined_apr(today_df: "pd.DataFrame", pc_factor: float = 0.66) -> "Tuple[Optional[float], str]":
        """ステップ6・7: 本日のSmartVault_Historyエントリから合算APRを算出する。

        ルール:
        - モバイルエントリが1件でもあれば → モバイルAPRの平均を採用
        - PCエントリのみ → PC APR平均 × pc_factor（デフォルト0.66）
        - データなし → (None, "本日のデータなし")
        """
        if today_df is None or today_df.empty:
            return None, "本日のデータなし"

        df = today_df.copy()
        df["APR"] = U.to_num_series(df["APR"])

        if "Device_Type" in df.columns:
            mobile_mask = df["Device_Type"].astype(str).str.strip().str.lower().isin(["mobile", "モバイル"])
            pc_mask = df["Device_Type"].astype(str).str.strip().str.lower() == "pc"
            mobile_df = df[mobile_mask]
            pc_df = df[pc_mask]
        else:
            mobile_df = pd.DataFrame()
            pc_df = df

        if not mobile_df.empty:
            mobile_avg = float(mobile_df["APR"].mean())
            if not pc_df.empty:
                pc_avg = float(pc_df["APR"].mean())
                expl = (
                    f"📱モバイル平均 {mobile_avg:.4f}% ／ 🖥️PC平均 {pc_avg:.4f}%"
                    f" → モバイル値を採用"
                )
            else:
                expl = f"📱モバイル平均 {mobile_avg:.4f}%"
            return mobile_avg, expl
        elif not pc_df.empty:
            pc_avg = float(pc_df["APR"].mean())
            combined = pc_avg * pc_factor
            expl = (
                f"🖥️PC平均 {pc_avg:.4f}% × {pc_factor:.0%}（PC専用補正）"
                f" = {combined:.4f}%"
            )
            return combined, expl
        else:
            avg = float(df["APR"].mean())
            return avg, f"全エントリ平均（デバイス情報なし）: {avg:.4f}%"


# =========================================================
# AUTH
# =========================================================
@dataclass
class AdminUser:
    name: str
    pin: str
    namespace: str


class AdminAuth:
    @staticmethod
    def load_users() -> List[AdminUser]:
        admin = st.secrets.get("admin", {}) or {}
        users = admin.get("users")
        if users:
            out: List[AdminUser] = []
            for u in users:
                name = str(u.get("name", "")).strip() or "Admin"
                pin = str(u.get("pin", "")).strip()
                ns = str(u.get("namespace", "")).strip() or name
                if pin:
                    out.append(AdminUser(name=name, pin=pin, namespace=ns))
            if out:
                return out

        pin = str(admin.get("pin", "")).strip() or str(admin.get("password", "")).strip()
        return [AdminUser(name="Admin", pin=pin, namespace="default")] if pin else []

    @staticmethod
    def require_login() -> None:
        admins = AdminAuth.load_users()
        if not admins:
            st.error("Secrets に [admin].users または [admin].pin が未設定です。")
            st.stop()

        if st.session_state.get("admin_ok") and st.session_state.get("admin_namespace"):
            return

        names = [a.name for a in admins]
        default_name = st.session_state.get("login_admin_name", names[0])
        if default_name not in names:
            default_name = names[0]

        st.markdown("## 🔐 管理者ログイン")
        with st.form("admin_gate_multi", clear_on_submit=False):
            admin_name = st.selectbox("管理者を選択", names, index=names.index(default_name))
            pw = st.text_input("管理者PIN", type="password")
            ok = st.form_submit_button("ログイン")
            if ok:
                st.session_state["login_admin_name"] = admin_name
                picked = next((a for a in admins if a.name == admin_name), None)
                if not picked:
                    st.error("管理者が見つかりません。")
                    st.stop()
                if pw == picked.pin:
                    st.session_state["admin_ok"] = True
                    st.session_state["admin_name"] = picked.name
                    st.session_state["admin_namespace"] = picked.namespace
                    st.rerun()
                st.session_state["admin_ok"] = False
                st.session_state["admin_name"] = ""
                st.session_state["admin_namespace"] = ""
                st.error("PINが違います。")
        st.stop()

    @staticmethod
    def current_label() -> str:
        name = str(st.session_state.get("admin_name", "")).strip() or "Admin"
        ns = str(st.session_state.get("admin_namespace", "")).strip() or "default"
        return f"{name}（namespace: {ns}）"

    @staticmethod
    def current_name() -> str:
        return str(st.session_state.get("admin_name", "")).strip() or "Admin"

    @staticmethod
    def current_namespace() -> str:
        return str(st.session_state.get("admin_namespace", "")).strip() or "default"


# =========================================================
# EXTERNAL SERVICE
# =========================================================
class ExternalService:
    @staticmethod
    def get_line_token(ns: str) -> str:
        line = st.secrets.get("line", {}) or {}
        tokens = line.get("tokens")
        if tokens:
            tok = str(tokens.get(ns, "")).strip()
            if tok:
                return tok
        legacy = str(line.get("channel_access_token", "")).strip()
        if legacy:
            return legacy
        st.error("LINEトークンが未設定です。")
        st.stop()

    @staticmethod
    def send_line_push(token: str, user_id: str, text: str, image_url: Optional[str] = None) -> int:
        if not user_id:
            return 400

        url = "https://api.line.me/v2/bot/message/push"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
        messages = [{"type": "text", "text": text}]
        if image_url:
            messages.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})

        try:
            r = requests.post(url, headers=headers, data=json.dumps({"to": str(user_id), "messages": messages}), timeout=25)
            return r.status_code
        except Exception:
            return 500

    @staticmethod
    def upload_imgbb(file_bytes: bytes) -> Optional[str]:
        try:
            key = st.secrets["imgbb"]["api_key"]
        except Exception:
            return None

        try:
            res = requests.post("https://api.imgbb.com/1/upload", params={"key": key}, files={"image": file_bytes}, timeout=30)
            return res.json()["data"]["url"]
        except Exception:
            return None

    @staticmethod
    def ocr_space_extract_text_with_crop(
        file_bytes: bytes,
        crop_left_ratio: float,
        crop_top_ratio: float,
        crop_right_ratio: float,
        crop_bottom_ratio: float,
        language: str = "eng",
        fast: bool = False,
    ) -> str:
        """fast=True のとき engine=1 のみ・前処理なしで呼び出す（USDC全画面OCR用）。"""
        # ── API キー取得 ──
        try:
            api_key = st.secrets["ocrspace"]["api_key"]
        except Exception as e:
            return f"[OCRエラー] APIキー取得失敗: secrets['ocrspace']['api_key'] が見つかりません。({e})"

        if not api_key or str(api_key).strip() in ("", "YOUR_API_KEY"):
            return "[OCRエラー] APIキーが空または未設定です。Streamlit SecretsにAPIキーを設定してください。"

        api_errors: List[str] = []

        def _call_ocr(target_name: str, target_bytes: bytes, engine: int) -> str:
            """1回のOCR APIコール。テキストを返す。失敗時は空文字。"""
            try:
                res = requests.post(
                    "https://api.ocr.space/parse/image",
                    files={"filename": (target_name, target_bytes)},
                    data={
                        "apikey": str(api_key).strip(),
                        "language": language,
                        "isOverlayRequired": False,
                        "OCREngine": engine,
                        "scale": True,
                        "detectOrientation": True,
                        "isTable": False,
                    },
                    timeout=15,
                )
                # JSON以外のレスポンス（プレーンテキストエラーなど）に対応
                try:
                    data = res.json()
                except Exception:
                    raw = res.text[:300] if res.text else f"HTTP {res.status_code}"
                    api_errors.append(f"非JSONレスポンス(engine={engine}): {raw}")
                    return ""

                if not isinstance(data, dict):
                    api_errors.append(f"予期しないレスポンス形式(engine={engine}): {str(data)[:200]}")
                    return ""

                if data.get("IsErroredOnProcessing"):
                    err_msgs = data.get("ErrorMessage", [])
                    msg = " | ".join(err_msgs) if isinstance(err_msgs, list) else str(err_msgs)
                    api_errors.append(f"APIエラー(engine={engine}): {msg}")
                    return ""

                parts = []
                for p in data.get("ParsedResults") or []:
                    if isinstance(p, dict):
                        txt = str(p.get("ParsedText", "")).strip()
                        if txt:
                            parts.append(txt)
                return "\n".join(parts)

            except Exception as req_e:
                api_errors.append(f"リクエストエラー(engine={engine}): {req_e}")
                return ""

        try:
            cropped_bytes = U.crop_image_by_ratio(
                file_bytes=file_bytes,
                left_ratio=crop_left_ratio,
                top_ratio=crop_top_ratio,
                right_ratio=crop_right_ratio,
                bottom_ratio=crop_bottom_ratio,
            )

            if fast:
                # ── 高速モード: engine=1 のみ・前処理なし ──
                txt = _call_ocr("cropped.png", cropped_bytes, 1)
                if txt:
                    return txt
            else:
                # ── 通常モード: engine=2→1 の順に試す ──
                for engine in (2, 1):
                    txt = _call_ocr("cropped.png", cropped_bytes, engine)
                    if txt:
                        return txt

                # ── フォールバック: 2×拡大+コントラスト強調（JPEG）で再試行 ──
                try:
                    _fb_img = Image.open(BytesIO(cropped_bytes)).convert("L")
                    _fb_img = ImageOps.autocontrast(_fb_img)
                    _fb_img = ImageEnhance.Contrast(_fb_img).enhance(3.0)
                    _fb_img = _fb_img.resize((_fb_img.width * 2, _fb_img.height * 2))
                    _fb_buf = BytesIO()
                    _fb_img.save(_fb_buf, format="JPEG", quality=90)
                    fb_bytes = _fb_buf.getvalue()
                    txt = _call_ocr("enhanced.jpg", fb_bytes, 2)
                    if txt:
                        return txt
                except Exception:
                    pass

            # テキストが取れなかった場合はAPIエラーを返す
            if api_errors:
                unique_errs = list(dict.fromkeys(api_errors))
                return "[OCRエラー] " + " / ".join(unique_errs)

            return ""

        except Exception as e:
            return f"[OCRエラー] 予期しないエラー: {e}"


# =========================================================
# GSHEET SERVICE
# =========================================================
@dataclass
class SheetNames:
    SETTINGS: str
    MEMBERS: str
    LEDGER: str
    LINEUSERS: str
    APR_SUMMARY: str
    SMARTVAULT_HISTORY: str
    USDC_HISTORY: str


class GSheetService:
    def __init__(self, spreadsheet_id: str, namespace: str):
        self.spreadsheet_id = spreadsheet_id
        self.namespace = namespace
        self.names = SheetNames(
            SETTINGS=U.sheet_name(AppConfig.SHEET["SETTINGS"], namespace),
            MEMBERS=U.sheet_name(AppConfig.SHEET["MEMBERS"], namespace),
            LEDGER=U.sheet_name(AppConfig.SHEET["LEDGER"], namespace),
            LINEUSERS=U.sheet_name(AppConfig.SHEET["LINEUSERS"], namespace),
            APR_SUMMARY=U.sheet_name(AppConfig.SHEET["APR_SUMMARY"], namespace),
            SMARTVAULT_HISTORY=U.sheet_name(AppConfig.SHEET["SMARTVAULT_HISTORY"], namespace),
            USDC_HISTORY=U.sheet_name(AppConfig.SHEET["USDC_HISTORY"], namespace),
        )

        con = st.secrets.get("connections", {}).get("gsheets", {})
        creds_info = con.get("credentials")
        if not creds_info:
            st.error("Secrets に [connections.gsheets.credentials] がありません。")
            st.stop()

        creds = Credentials.from_service_account_info(
            dict(creds_info),
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        self.creds = creds  # Drive アップロードで再利用
        self.gc = gspread.authorize(creds)
        self.book = self.gc.open_by_key(self.spreadsheet_id)

        # ヘッダー定義が変わったら（列追加など）必ず再実行されるよう、
        # ヘッダー内容のハッシュを key に含める
        import hashlib as _hashlib, json as _json
        _headers_sig = _hashlib.md5(
            _json.dumps(AppConfig.HEADERS, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()[:10]
        ensure_key = (
            f"_sheet_ensured_{_headers_sig}_{self.names.SETTINGS}_{self.names.MEMBERS}_{self.names.LEDGER}_"
            f"{self.names.LINEUSERS}_{self.names.APR_SUMMARY}_{self.names.SMARTVAULT_HISTORY}_"
            f"{self.names.USDC_HISTORY}"
        )
        if not st.session_state.get(ensure_key, False):
            for key in AppConfig.HEADERS:
                self.ensure_sheet(key)
            st.session_state[ensure_key] = True

    def actual_name(self, key: str) -> str:
        return getattr(self.names, key)

    def ws(self, key_or_name: str):
        name = self.actual_name(key_or_name) if hasattr(self.names, key_or_name) else key_or_name
        return self.book.worksheet(name)

    def spreadsheet_url(self) -> str:
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"

    def ensure_sheet(self, key: str) -> None:
        name = self.actual_name(key)
        headers = AppConfig.HEADERS[key]
        try:
            ws = self.ws(key)
        except Exception:
            ws = self.book.add_worksheet(title=name, rows=3000, cols=max(30, len(headers) + 10))
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        try:
            first = ws.row_values(1)
        except APIError:
            return

        if not first:
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return

        colset = [str(c).strip() for c in first if str(c).strip()]
        missing = [h for h in headers if h not in colset]
        if missing:
            try:
                # update() の引数順は gspread バージョンによって異なるため、
                # 互換性の高い update_cell を1セルずつ書き込む
                for _i, _h in enumerate(missing):
                    ws.update_cell(1, len(colset) + 1 + _i, _h)
            except Exception:
                # ヘッダー追加失敗は警告扱い（アプリ起動はブロックしない）
                pass

    @st.cache_data(ttl=600)
    def load_df(_self, key: str) -> pd.DataFrame:
        try:
            values = _self.ws(key).get_all_values()
        except APIError as e:
            raise RuntimeError(f"Google Sheets 読み取りエラー: {_self.actual_name(key)} を取得できません。") from e
        except Exception as e:
            raise RuntimeError(f"{_self.actual_name(key)} の読み取り中にエラーが発生しました: {e}") from e

        if not values:
            return pd.DataFrame()

        return U.clean_cols(pd.DataFrame(values[1:], columns=values[0]))

    def write_df(self, key: str, df: pd.DataFrame) -> None:
        ws = self.ws(key)
        out = df.fillna("").astype(str)
        ws.clear()
        ws.update([out.columns.tolist()] + out.values.tolist(), value_input_option="USER_ENTERED")

    def append_row(self, key: str, row: List[Any]) -> None:
        try:
            self.ws(key).append_row([("" if x is None else x) for x in row], value_input_option="USER_ENTERED")
        except Exception as e:
            raise RuntimeError(f"{self.actual_name(key)} への追記に失敗しました: {e}")

    def overwrite_rows(self, key: str, rows: List[List[Any]]) -> None:
        ws = self.ws(key)
        ws.clear()
        ws.update(rows, value_input_option="USER_ENTERED")

    def clear_cache(self) -> None:
        st.cache_data.clear()

    def upload_image_to_drive(
        self,
        file_bytes: bytes,
        filename: str,
        mime_type: str = "image/jpeg",
    ) -> Optional[str]:
        """画像を Google Drive にアップロードして公開 URL を返す。
        失敗時は None を返す（アプリの動作は継続）。
        """
        try:
            import json as _json
            from google.auth.transport.requests import Request as _GReq

            # アクセストークンを更新
            if not self.creds.valid:
                self.creds.refresh(_GReq())
            token = self.creds.token

            # ── マルチパートアップロード ──────────────────────────────────
            boundary = "===APRUploadBoundary==="
            meta = _json.dumps({"name": filename, "mimeType": mime_type})
            body = (
                f"--{boundary}\r\n"
                f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
                f"{meta}\r\n"
                f"--{boundary}\r\n"
                f"Content-Type: {mime_type}\r\n\r\n"
            ).encode("utf-8") + file_bytes + f"\r\n--{boundary}--".encode("utf-8")

            upload_resp = requests.post(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": f"multipart/related; boundary={boundary}",
                },
                data=body,
                timeout=60,
            )
            upload_resp.raise_for_status()
            file_id = upload_resp.json()["id"]

            # ── 誰でも閲覧できるよう公開設定 ──────────────────────────────
            requests.post(
                f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"role": "reader", "type": "anyone"},
                timeout=15,
            )

            # スプレッドシートの =IMAGE() で直接表示できる URL
            return f"https://drive.google.com/uc?export=view&id={file_id}"
        except Exception:
            return None


# =========================================================
# REPOSITORY
# =========================================================
class Repository:
    def __init__(self, gs: GSheetService):
        self.gs = gs

    def _ensure_setting_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        all_defaults = {
            **AppConfig.OCR_DEFAULTS_PC,
            **AppConfig.OCR_DEFAULTS_MOBILE,
            **AppConfig.SV_BOX_DEFAULTS,
            **AppConfig.PC_BOX_DEFAULTS,
        }
        for k, v in all_defaults.items():
            if k not in out.columns:
                out[k] = v
            else:
                out[k] = out[k].replace("", v)
        return out

    def load_settings(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("SETTINGS")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["SETTINGS"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["SETTINGS"])

        for c in AppConfig.HEADERS["SETTINGS"]:
            if c not in df.columns:
                df[c] = ""

        df = df[AppConfig.HEADERS["SETTINGS"]].copy()
        df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
        df = df[df["Project_Name"] != ""].copy()
        df["Net_Factor"] = U.to_num_series(df["Net_Factor"], AppConfig.FACTOR["MASTER"])
        df.loc[df["Net_Factor"] <= 0, "Net_Factor"] = AppConfig.FACTOR["MASTER"]
        df["IsCompound"] = U.truthy_series(df["IsCompound"])
        df["Compound_Timing"] = df["Compound_Timing"].apply(U.normalize_compound)
        df["Active"] = df["Active"].apply(lambda x: U.truthy(x) if str(x).strip() else True)
        df["UpdatedAt_JST"] = df["UpdatedAt_JST"].astype(str).str.strip()

        all_ocr_defaults = {
            **AppConfig.OCR_DEFAULTS_PC,
            **AppConfig.OCR_DEFAULTS_MOBILE,
            **AppConfig.SV_BOX_DEFAULTS,
            **AppConfig.PC_BOX_DEFAULTS,
        }
        for k, v in all_ocr_defaults.items():
            if k in df.columns:
                df[k] = df[k].apply(lambda x, default=v: U.to_ratio(x, default))
            else:
                df[k] = v

        personal_df = df[df["Project_Name"].str.upper() == AppConfig.PROJECT["PERSONAL"]].tail(1).copy()
        other_df = df[df["Project_Name"].str.upper() != AppConfig.PROJECT["PERSONAL"]].drop_duplicates(subset=["Project_Name"], keep="last")
        out = pd.concat([personal_df, other_df], ignore_index=True)

        if AppConfig.PROJECT["PERSONAL"] not in out["Project_Name"].astype(str).tolist():
            out = pd.concat(
                [
                    pd.DataFrame(
                        [
                            {
                                "Project_Name": AppConfig.PROJECT["PERSONAL"],
                                "Net_Factor": AppConfig.FACTOR["MASTER"],
                                "IsCompound": True,
                                "Compound_Timing": AppConfig.COMPOUND["DAILY"],
                                "Crop_Left_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"],
                                "Crop_Top_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"],
                                "Crop_Right_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"],
                                "Crop_Bottom_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"],
                                "Crop_Left_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"],
                                "Crop_Top_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"],
                                "Crop_Right_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"],
                                "Crop_Bottom_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"],
                                **AppConfig.SV_BOX_DEFAULTS,
                                **AppConfig.PC_BOX_DEFAULTS,
                                "UpdatedAt_JST": U.fmt_dt(U.now_jst()),
                                "Active": True,
                            }
                        ]
                    ),
                    out,
                ],
                ignore_index=True,
            )

        return self._ensure_setting_defaults(out)

    def write_settings(self, df: pd.DataFrame) -> None:
        out = df.copy()
        for c in AppConfig.HEADERS["SETTINGS"]:
            if c not in out.columns:
                out[c] = ""
        out = out[AppConfig.HEADERS["SETTINGS"]].copy()
        out["Project_Name"] = out["Project_Name"].astype(str).str.strip()
        out = out[out["Project_Name"] != ""].copy()
        out["Net_Factor"] = U.to_num_series(out["Net_Factor"], AppConfig.FACTOR["MASTER"]).map(lambda x: f"{float(x):.2f}")
        out["IsCompound"] = out["IsCompound"].apply(lambda x: "TRUE" if U.truthy(x) else "FALSE")
        out["Compound_Timing"] = out["Compound_Timing"].apply(U.normalize_compound)

        for k, v in {**AppConfig.OCR_DEFAULTS_PC, **AppConfig.OCR_DEFAULTS_MOBILE, **AppConfig.SV_BOX_DEFAULTS, **AppConfig.PC_BOX_DEFAULTS}.items():
            if k in out.columns:
                out[k] = out[k].apply(lambda x, default=v: f"{U.to_ratio(x, default):.3f}")
            else:
                out[k] = f"{v:.3f}"

        out["Active"] = out["Active"].apply(lambda x: "TRUE" if U.truthy(x) else "FALSE")
        out["UpdatedAt_JST"] = out["UpdatedAt_JST"].astype(str)
        self.gs.write_df("SETTINGS", out)

    def repair_settings(self, settings_df: pd.DataFrame) -> pd.DataFrame:
        repaired = settings_df.copy()
        before_count = len(repaired)

        if repaired.empty:
            repaired = pd.DataFrame(columns=AppConfig.HEADERS["SETTINGS"])

        for c in AppConfig.HEADERS["SETTINGS"]:
            if c not in repaired.columns:
                repaired[c] = ""

        repaired = self._ensure_setting_defaults(repaired)
        repaired["Project_Name"] = repaired["Project_Name"].astype(str).str.strip()
        repaired = repaired[repaired["Project_Name"] != ""].copy()

        personal_df = repaired[repaired["Project_Name"].str.upper() == AppConfig.PROJECT["PERSONAL"]].tail(1).copy()
        other_df = repaired[repaired["Project_Name"].str.upper() != AppConfig.PROJECT["PERSONAL"]].drop_duplicates(subset=["Project_Name"], keep="last")
        repaired = pd.concat([personal_df, other_df], ignore_index=True)

        repaired["Net_Factor"] = U.to_num_series(repaired["Net_Factor"], AppConfig.FACTOR["MASTER"])
        repaired.loc[repaired["Net_Factor"] <= 0, "Net_Factor"] = AppConfig.FACTOR["MASTER"]
        repaired["IsCompound"] = repaired["IsCompound"].apply(U.truthy)
        repaired["Compound_Timing"] = repaired["Compound_Timing"].apply(U.normalize_compound)
        repaired["Active"] = repaired["Active"].apply(lambda x: U.truthy(x) if str(x).strip() else True)
        repaired["UpdatedAt_JST"] = repaired["UpdatedAt_JST"].astype(str) if "UpdatedAt_JST" in repaired.columns else ""

        all_ocr_defaults_r = {**AppConfig.OCR_DEFAULTS_PC, **AppConfig.OCR_DEFAULTS_MOBILE, **AppConfig.SV_BOX_DEFAULTS, **AppConfig.PC_BOX_DEFAULTS}
        for k, v in all_ocr_defaults_r.items():
            if k in repaired.columns:
                repaired[k] = repaired[k].apply(lambda x, default=v: U.to_ratio(x, default))
            else:
                repaired[k] = v

        if AppConfig.PROJECT["PERSONAL"] not in repaired["Project_Name"].astype(str).tolist():
            repaired = pd.concat(
                [
                    pd.DataFrame(
                        [
                            {
                                "Project_Name": AppConfig.PROJECT["PERSONAL"],
                                "Net_Factor": AppConfig.FACTOR["MASTER"],
                                "IsCompound": True,
                                "Compound_Timing": AppConfig.COMPOUND["DAILY"],
                                "Crop_Left_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"],
                                "Crop_Top_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"],
                                "Crop_Right_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"],
                                "Crop_Bottom_Ratio_PC": AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"],
                                "Crop_Left_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"],
                                "Crop_Top_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"],
                                "Crop_Right_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"],
                                "Crop_Bottom_Ratio_Mobile": AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"],
                                **AppConfig.SV_BOX_DEFAULTS,
                                **AppConfig.PC_BOX_DEFAULTS,
                                "UpdatedAt_JST": U.fmt_dt(U.now_jst()),
                                "Active": True,
                            }
                        ]
                    ),
                    repaired,
                ],
                ignore_index=True,
            )

        need_write = len(repaired) != before_count or settings_df.empty
        try:
            left = repaired[AppConfig.HEADERS["SETTINGS"]].astype(str).reset_index(drop=True)
            right = settings_df.reindex(columns=AppConfig.HEADERS["SETTINGS"]).astype(str).reset_index(drop=True)
            if not left.equals(right):
                need_write = True
        except Exception:
            need_write = True

        if need_write:
            self.write_settings(repaired)
            self.gs.clear_cache()
            repaired = self.load_settings()

        return repaired

    def load_members(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("MEMBERS")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["MEMBERS"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["MEMBERS"])

        for c in AppConfig.HEADERS["MEMBERS"]:
            if c not in df.columns:
                df[c] = ""

        df["Project_Name"] = df["Project_Name"].astype(str).str.strip()
        df["PersonName"] = df["PersonName"].astype(str).str.strip()
        df["Principal"] = U.to_num_series(df["Principal"])
        df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
        df["LINE_DisplayName"] = df["LINE_DisplayName"].astype(str).str.strip()
        df["Rank"] = df["Rank"].apply(U.normalize_rank)
        df["IsActive"] = df["IsActive"].apply(U.truthy)
        return df

    def write_members(self, members_df: pd.DataFrame) -> None:
        out = members_df.copy()
        out["Principal"] = U.to_num_series(out["Principal"]).map(lambda x: f"{float(x):.6f}")
        out["IsActive"] = out["IsActive"].apply(lambda x: "TRUE" if U.truthy(x) else "FALSE")
        out["Rank"] = out["Rank"].apply(U.normalize_rank)
        self.gs.write_df("MEMBERS", out)

    def load_ledger(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("LEDGER")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["LEDGER"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["LEDGER"])

        for c in AppConfig.HEADERS["LEDGER"]:
            if c not in df.columns:
                df[c] = ""
        df["Amount"] = U.to_num_series(df["Amount"])
        return df

    def load_line_users(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("LINEUSERS")
        except Exception as e:
            st.error(str(e))
            return pd.DataFrame(columns=AppConfig.HEADERS["LINEUSERS"])

        if df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["LINEUSERS"])

        if "Line_User_ID" not in df.columns and "LineID" in df.columns:
            df = df.rename(columns={"LineID": "Line_User_ID"})
        if "Line_User" not in df.columns and "LINE_DisplayName" in df.columns:
            df = df.rename(columns={"LINE_DisplayName": "Line_User"})

        if "Line_User_ID" not in df.columns:
            df["Line_User_ID"] = ""
        if "Line_User" not in df.columns:
            df["Line_User"] = ""

        df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
        df["Line_User"] = df["Line_User"].astype(str).str.strip()
        return df

    def write_apr_summary(self, summary_df: pd.DataFrame) -> None:
        if summary_df.empty:
            return
        out = summary_df.copy()
        out["Date_JST"] = out["Date_JST"].astype(str)
        out["PersonName"] = out["PersonName"].astype(str)
        out["Total_APR"] = U.to_num_series(out["Total_APR"]).map(lambda x: f"{float(x):.6f}")
        out["APR_Count"] = U.to_num_series(out["APR_Count"]).astype(int).astype(str)
        out["Asset_Ratio"] = out["Asset_Ratio"].astype(str)
        out["LINE_DisplayName"] = out["LINE_DisplayName"].astype(str)
        self.gs.write_df("APR_SUMMARY", out)

    def append_ledger(
        self,
        dt_jst: str,
        project: str,
        person_name: str,
        typ: str,
        amount: float,
        note: str,
        evidence_url: str = "",
        line_user_id: str = "",
        line_display_name: str = "",
        source: str = AppConfig.SOURCE["APP"],
    ) -> None:
        if not str(project).strip():
            raise ValueError("project が空です")
        if not str(person_name).strip():
            raise ValueError("person_name が空です")
        if not str(typ).strip():
            raise ValueError("typ が空です")
        self.gs.append_row(
            "LEDGER",
            [dt_jst, project, person_name, typ, float(amount), note, evidence_url or "", line_user_id or "", line_display_name or "", source],
        )

    def append_smartvault_history(
        self,
        dt_jst: str,
        project: str,
        liquidity: float,
        yesterday_profit: float,
        apr: float,
        source_mode: str,
        ocr_liquidity: Optional[float],
        ocr_yesterday_profit: Optional[float],
        ocr_apr: Optional[float],
        evidence_url: str,
        admin_name: str,
        admin_namespace: str,
        note: str = "",
        device_type: str = "",  # ステップ3: "pc" or "mobile"
    ) -> None:
        self.gs.append_row(
            "SMARTVAULT_HISTORY",
            [
                dt_jst,
                project,
                float(liquidity),
                float(yesterday_profit),
                float(apr),
                str(source_mode),
                "" if ocr_liquidity is None else float(ocr_liquidity),
                "" if ocr_yesterday_profit is None else float(ocr_yesterday_profit),
                "" if ocr_apr is None else float(ocr_apr),
                evidence_url or "",
                admin_name or "",
                admin_namespace or "",
                note or "",
                str(device_type),  # Device_Type 列
            ],
        )

    def append_usdc_history_rows(
        self,
        rows: List[Dict[str, Any]],
        project: str,
        admin_name: str,
        admin_namespace: str,
        note: str = "",
    ) -> Tuple[int, int]:
        """Write USDC transaction history rows (from OCR) to USDC_History sheet.

        Skips rows that already exist (duplicate = Source_Project + Date_Label + Time_Label + Amount_USD).
        Returns (written_count, skipped_count).
        """
        # Load existing rows to build duplicate key set
        existing_keys: Set[Tuple[str, str, str, str]] = set()
        try:
            existing_df = self.gs.load_df("USDC_HISTORY")
            if not existing_df.empty:
                for _, ex in existing_df.iterrows():
                    key = (
                        str(ex.get("Source_Project", "")).strip(),
                        str(ex.get("Date_Label", "")).strip(),
                        str(ex.get("Time_Label", "")).strip(),
                        str(ex.get("Amount_USD", "")).strip(),
                    )
                    existing_keys.add(key)
        except Exception:
            pass  # シートが空 or 読み取りエラーは無視して全件書き込み

        created_at = U.fmt_dt(U.now_jst())
        written, skipped = 0, 0
        for r in rows:
            date_label = str(r.get("date_str", "")).strip()
            time_label = str(r.get("time_str", "")).strip()
            amount_usd = float(r.get("amount", 0.0))
            key = (
                str(project).strip(),
                date_label,
                time_label,
                str(amount_usd).strip(),
            )
            if key in existing_keys:
                skipped += 1
                continue
            unique_key = f"{str(project).strip()}_{date_label}_{time_label}_{amount_usd}"
            self.gs.append_row(
                "USDC_HISTORY",
                [
                    unique_key,
                    date_label,
                    time_label,
                    "received",
                    amount_usd,
                    amount_usd,   # Token_Amount（USDC は 1:1）
                    "USDC",
                    note or "",   # Source_Image（証跡URLがあれば）
                    str(project),
                    r.get("datetime_jst", ""),  # OCR_Raw_Text の代わりに datetime を格納
                    created_at,
                ],
            )
            existing_keys.add(key)  # 同一実行内の重複も防ぐ
            written += 1
        return written, skipped

    def active_projects(self, settings_df: pd.DataFrame) -> List[str]:
        if settings_df.empty:
            return []
        return settings_df.loc[settings_df["Active"] == True, "Project_Name"].dropna().astype(str).unique().tolist()

    def project_members_active(self, members_df: pd.DataFrame, project: str) -> pd.DataFrame:
        if members_df.empty:
            return members_df.copy()
        return members_df[(members_df["Project_Name"] == str(project)) & (members_df["IsActive"] == True)].copy().reset_index(drop=True)

    def validate_no_dup_lineid(self, members_df: pd.DataFrame, project: str) -> Optional[str]:
        if members_df.empty:
            return None
        df = members_df[members_df["Project_Name"] == str(project)].copy()
        df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
        df = df[df["Line_User_ID"] != ""]
        dup = df[df.duplicated(subset=["Line_User_ID"], keep=False)]
        return None if dup.empty else f"同一プロジェクト内で Line_User_ID が重複しています: {dup['Line_User_ID'].unique().tolist()}"

    def load_today_smartvault_history(self, date_jst: str, project: str) -> "pd.DataFrame":
        """ステップ5: 本日のSmartVault_Historyエントリを取得する（日次積み上げ確認用）。"""
        try:
            df = self.gs.load_df("SMARTVAULT_HISTORY")
            if df.empty:
                return pd.DataFrame()
            mask = (
                df["Datetime_JST"].astype(str).str.startswith(str(date_jst))
                & (df["Project_Name"].astype(str).str.strip() == str(project).strip())
            )
            return df[mask].copy()
        except Exception:
            return pd.DataFrame()

    def existing_apr_keys_for_date(self, date_jst: str) -> Set[Tuple[str, str]]:
        ledger_df = self.load_ledger()
        if ledger_df.empty:
            return set()
        df = ledger_df[
            (ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"])
            & (ledger_df["Datetime_JST"].astype(str).str.startswith(date_jst))
        ].copy()
        if df.empty:
            return set()
        return set(zip(df["Project_Name"].astype(str).str.strip(), df["PersonName"].astype(str).str.strip()))

    def reset_today_apr_records(self, date_jst: str, project: str) -> Tuple[int, int]:
        ws = self.gs.ws("LEDGER")
        values = ws.get_all_values()
        if not values:
            return 0, 0

        headers = values[0]
        if len(values) == 1:
            return 0, 0

        need_cols = ["Datetime_JST", "Project_Name", "Type", "Note"]
        if any(c not in headers for c in need_cols):
            return 0, 0

        idx_dt = headers.index("Datetime_JST")
        idx_project = headers.index("Project_Name")
        idx_type = headers.index("Type")
        idx_note = headers.index("Note")
        kept_rows, deleted_apr, deleted_line = [headers], 0, 0

        for row in values[1:]:
            row = row + [""] * (len(headers) - len(row))
            dt_v = str(row[idx_dt]).strip()
            project_v = str(row[idx_project]).strip()
            type_v = str(row[idx_type]).strip()
            note_v = str(row[idx_note]).strip()

            is_today = dt_v.startswith(date_jst)
            is_project = project_v == str(project).strip()
            delete_apr = is_today and is_project and type_v == AppConfig.TYPE["APR"]
            delete_line = is_today and is_project and type_v == AppConfig.TYPE["LINE"] and AppConfig.APR_LINE_NOTE_KEYWORD in note_v

            if delete_apr:
                deleted_apr += 1
                continue
            if delete_line:
                deleted_line += 1
                continue
            kept_rows.append(row[: len(headers)])

        if deleted_apr > 0 or deleted_line > 0:
            self.gs.overwrite_rows("LEDGER", kept_rows)
            self.gs.clear_cache()

        return deleted_apr, deleted_line


# =========================================================
# FINANCE ENGINE
# =========================================================
class FinanceEngine:
    def calc_project_apr(self, mem: pd.DataFrame, apr_percent: float, project_net_factor: float, project_name: str) -> pd.DataFrame:
        out = mem.copy()
        if str(project_name).strip().upper() == AppConfig.PROJECT["PERSONAL"]:
            out["Factor"] = out["Rank"].map(U.rank_factor)
            out["DailyAPR"] = (out["Principal"] * (apr_percent / 100.0) * out["Factor"]) / 365.0
            out["CalcMode"] = "PERSONAL"
            return out

        total_principal = float(out["Principal"].sum())
        count = len(out)
        factor = float(project_net_factor if project_net_factor > 0 else AppConfig.FACTOR["MASTER"])
        total_group_reward = (total_principal * (apr_percent / 100.0) * factor) / 365.0
        out["Factor"] = factor
        out["DailyAPR"] = (total_group_reward / count) if count > 0 else 0.0
        out["CalcMode"] = "GROUP_EQUAL"
        return out

    def build_apr_summary(self, ledger_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
        if ledger_df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["APR_SUMMARY"])

        apr_df = ledger_df[ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"]].copy()
        if apr_df.empty:
            return pd.DataFrame(columns=AppConfig.HEADERS["APR_SUMMARY"])

        apr_df["PersonName"] = apr_df["PersonName"].astype(str).str.strip()
        apr_df["LINE_DisplayName"] = apr_df["LINE_DisplayName"].astype(str).str.strip()
        apr_df["Amount"] = U.to_num_series(apr_df["Amount"])

        active_mem = members_df[members_df["IsActive"] == True].copy() if not members_df.empty and "IsActive" in members_df.columns else members_df.copy()
        total_assets = float(active_mem["Principal"].sum()) if not active_mem.empty else 0.0

        summary = apr_df.groupby("PersonName", as_index=False).agg(Total_APR=("Amount", "sum"), APR_Count=("Amount", "count"))
        disp_map = apr_df.sort_values("Datetime_JST", ascending=False).drop_duplicates(subset=["PersonName"])[["PersonName", "LINE_DisplayName"]].copy()
        summary = summary.merge(disp_map, on="PersonName", how="left")
        summary["Date_JST"] = U.fmt_date(U.now_jst())
        summary["Asset_Ratio"] = summary["Total_APR"].map(lambda x: f"{(float(x) / total_assets) * 100:.2f}%" if total_assets > 0 else "0.00%")
        return summary[["Date_JST", "PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"]].copy()

    def apply_monthly_compound(self, repo: Repository, members_df: pd.DataFrame, project: str) -> Tuple[int, float]:
        ledger_df = repo.load_ledger()
        if ledger_df.empty:
            return 0, 0.0

        target = ledger_df[
            (ledger_df["Project_Name"].astype(str).str.strip() == str(project).strip())
            & (ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"])
            & (~ledger_df["Note"].astype(str).str.contains("COMPOUNDED", na=False))
        ].copy()
        if target.empty:
            return 0, 0.0

        sums = target.groupby("PersonName", as_index=False)["Amount"].sum()
        if sums.empty:
            return 0, 0.0

        ts = U.fmt_dt(U.now_jst())
        updated_count, total_added = 0, 0.0
        add_map = dict(zip(sums["PersonName"].astype(str).str.strip(), U.to_num_series(sums["Amount"])))
        mask = (members_df["Project_Name"].astype(str).str.strip() == str(project).strip()) & (
            members_df["PersonName"].astype(str).str.strip().isin(add_map.keys())
        )

        if mask.any():
            for idx in members_df[mask].index.tolist():
                person = str(members_df.loc[idx, "PersonName"]).strip()
                addv = float(add_map.get(person, 0.0))
                if addv == 0:
                    continue
                members_df.loc[idx, "Principal"] = float(members_df.loc[idx, "Principal"]) + addv
                members_df.loc[idx, "UpdatedAt_JST"] = ts
                updated_count += 1
                total_added += addv

        if updated_count > 0:
            repo.write_members(members_df)
            ws = repo.gs.ws("LEDGER")
            values = ws.get_all_values()
            if values and len(values) >= 2:
                headers = values[0]
                note_idx = headers.index("Note") + 1 if "Note" in headers else None
                if note_idx:
                    for row_no in range(2, len(values) + 1):
                        row = values[row_no - 1]
                        if len(row) < len(headers):
                            row = row + [""] * (len(headers) - len(row))
                        r_project = str(row[headers.index("Project_Name")]).strip()
                        r_type = str(row[headers.index("Type")]).strip()
                        r_note = str(row[headers.index("Note")]).strip()
                        if r_project == str(project).strip() and r_type == AppConfig.TYPE["APR"] and "COMPOUNDED" not in r_note:
                            ws.update_cell(row_no, note_idx, (r_note + " | " if r_note else "") + f"COMPOUNDED:{ts}")
            repo.gs.clear_cache()

        return updated_count, total_added


# =========================================================
# DATA STORE
# =========================================================
class DataStore:
    def __init__(self, repo: Repository, engine: FinanceEngine):
        self.repo = repo
        self.engine = engine

    def clear(self) -> None:
        for key in AppConfig.SESSION_KEYS.values():
            if key in st.session_state:
                del st.session_state[key]

    def load(self, force: bool = False) -> Dict[str, pd.DataFrame]:
        if force or AppConfig.SESSION_KEYS["SETTINGS"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["SETTINGS"]] = self.repo.repair_settings(self.repo.load_settings())
        if force or AppConfig.SESSION_KEYS["MEMBERS"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["MEMBERS"]] = self.repo.load_members()
        if force or AppConfig.SESSION_KEYS["LEDGER"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["LEDGER"]] = self.repo.load_ledger()
        if force or AppConfig.SESSION_KEYS["LINEUSERS"] not in st.session_state:
            st.session_state[AppConfig.SESSION_KEYS["LINEUSERS"]] = self.repo.load_line_users()

        settings_df = st.session_state[AppConfig.SESSION_KEYS["SETTINGS"]]
        members_df = st.session_state[AppConfig.SESSION_KEYS["MEMBERS"]]
        ledger_df = st.session_state[AppConfig.SESSION_KEYS["LEDGER"]]
        line_users_df = st.session_state[AppConfig.SESSION_KEYS["LINEUSERS"]]
        apr_summary_df = self.engine.build_apr_summary(ledger_df, members_df)
        st.session_state[AppConfig.SESSION_KEYS["APR_SUMMARY"]] = apr_summary_df

        return {
            "settings_df": settings_df,
            "members_df": members_df,
            "ledger_df": ledger_df,
            "line_users_df": line_users_df,
            "apr_summary_df": apr_summary_df,
        }

    def refresh(self) -> Dict[str, pd.DataFrame]:
        self.repo.gs.clear_cache()
        self.clear()
        return self.load(force=True)

    def persist_and_refresh(self) -> Dict[str, pd.DataFrame]:
        data = self.refresh()
        self.repo.write_apr_summary(data["apr_summary_df"])
        return self.refresh()


# =========================================================
# UI
# =========================================================
class AppUI:
    def __init__(self, repo: Repository, engine: FinanceEngine, store: DataStore):
        self.repo = repo
        self.engine = engine
        self.store = store

    def _ocr_crop_text(self, file_bytes: bytes, box: Dict[str, float], language: str = "eng", fast: bool = False) -> str:
        return ExternalService.ocr_space_extract_text_with_crop(
            file_bytes=file_bytes,
            crop_left_ratio=box["left"],
            crop_top_ratio=box["top"],
            crop_right_ratio=box["right"],
            crop_bottom_ratio=box["bottom"],
            language=language,
            fast=fast,
        )

    @staticmethod
    def _expand_box(box: Dict[str, float], margin: float) -> Dict[str, float]:
        """Return a new box expanded by `margin` on all sides (clamped to [0, 1])."""
        return {
            "left": max(0.0, box["left"] - margin),
            "top": max(0.0, box["top"] - margin),
            "right": min(1.0, box["right"] + margin),
            "bottom": min(1.0, box["bottom"] + margin),
        }

    @staticmethod
    def _build_sv_boxes(srow: Optional[Any]) -> Dict[str, Dict[str, float]]:
        """Build SmartVault OCR boxes from a Settings row, falling back to defaults."""
        d = AppConfig.SV_BOX_DEFAULTS
        if srow is None:
            return AppConfig.SMARTVAULT_BOXES_MOBILE
        def _r(key: str) -> float:
            return U.to_ratio(srow.get(key, d[key]), d[key])
        return {
            "TOTAL_LIQUIDITY": {"left": _r("SV_Liq_Left"),    "top": _r("SV_Liq_Top"),    "right": _r("SV_Liq_Right"),    "bottom": _r("SV_Liq_Bottom")},
            "YESTERDAY_PROFIT": {"left": _r("SV_Profit_Left"), "top": _r("SV_Profit_Top"), "right": _r("SV_Profit_Right"), "bottom": _r("SV_Profit_Bottom")},
            "APR":              {"left": _r("SV_APR_Left"),    "top": _r("SV_APR_Top"),    "right": _r("SV_APR_Right"),    "bottom": _r("SV_APR_Bottom")},
        }

    def _ocr_smartvault_mobile_metrics(self, file_bytes: bytes, srow: Optional[Any] = None) -> Dict[str, Any]:
        """Extract 3 SmartVault metrics from a mobile screenshot.

        Uses boxes from ``srow`` (Settings row) when available, otherwise falls
        back to the hardcoded defaults.  If a value is not detected on the first
        pass, the corresponding box is automatically expanded by
        ``AppConfig.OCR_EXPAND_MARGIN`` and retried once.
        fast=True（engine=1 のみ）で API コール数を最小限に抑える。
        """
        boxes = self._build_sv_boxes(srow)
        margin = AppConfig.OCR_EXPAND_MARGIN

        total_text = self._ocr_crop_text(file_bytes, boxes["TOTAL_LIQUIDITY"])
        profit_text = self._ocr_crop_text(file_bytes, boxes["YESTERDAY_PROFIT"])
        apr_text = self._ocr_crop_text(file_bytes, boxes["APR"])

        total_vals = U.extract_usd_candidates(total_text)
        profit_vals = U.extract_usd_candidates(profit_text)
        apr_vals = U.extract_percent_candidates(apr_text)

        total_liquidity = U.pick_total_liquidity(total_vals)
        yesterday_profit = U.pick_yesterday_profit(profit_vals)
        apr_value = apr_vals[0] if apr_vals else None

        # --- Auto-expand fallback: retry once with wider boxes if value is None ---
        if total_liquidity is None:
            retry_text = self._ocr_crop_text(file_bytes, self._expand_box(boxes["TOTAL_LIQUIDITY"], margin))
            retry_vals = U.extract_usd_candidates(retry_text)
            total_liquidity = U.pick_total_liquidity(retry_vals) or total_liquidity
            if retry_vals:
                total_text += f"\n[retry] {retry_text}"
                total_vals = retry_vals

        if yesterday_profit is None:
            retry_text = self._ocr_crop_text(file_bytes, self._expand_box(boxes["YESTERDAY_PROFIT"], margin))
            retry_vals = U.extract_usd_candidates(retry_text)
            yesterday_profit = U.pick_yesterday_profit(retry_vals) or yesterday_profit
            if retry_vals:
                profit_text += f"\n[retry] {retry_text}"
                profit_vals = retry_vals

        if apr_value is None:
            retry_text = self._ocr_crop_text(file_bytes, self._expand_box(boxes["APR"], margin))
            retry_vals = U.extract_percent_candidates(retry_text)
            if retry_vals:
                apr_value = retry_vals[0]
                apr_text += f"\n[retry] {retry_text}"
                apr_vals = retry_vals

        boxed_preview = U.draw_ocr_boxes(file_bytes, boxes)

        return {
            "boxes": boxes,
            "total_text": total_text,
            "profit_text": profit_text,
            "apr_text": apr_text,
            "total_vals": total_vals,
            "profit_vals": profit_vals,
            "apr_vals": apr_vals,
            "total_liquidity": total_liquidity,
            "yesterday_profit": yesterday_profit,
            "apr_value": apr_value,
            "boxed_preview": boxed_preview,
        }

    @staticmethod
    def _build_pc_boxes(srow: Optional[Any], crop_left: float, crop_top: float, crop_right: float, crop_bottom: float) -> Dict[str, Dict[str, float]]:
        """Build PC 3-zone OCR boxes.

        Liquidity and profit zones come from Settings (PC_Liq_* / PC_Profit_*).
        APR zone uses the existing Crop_* values already resolved by the caller.
        """
        d = AppConfig.PC_BOX_DEFAULTS
        if srow is not None:
            def _r(key: str) -> float:
                return U.to_ratio(srow.get(key, d[key]), d[key])
            liq_box = {"left": _r("PC_Liq_Left"), "top": _r("PC_Liq_Top"), "right": _r("PC_Liq_Right"), "bottom": _r("PC_Liq_Bottom")}
            profit_box = {"left": _r("PC_Profit_Left"), "top": _r("PC_Profit_Top"), "right": _r("PC_Profit_Right"), "bottom": _r("PC_Profit_Bottom")}
        else:
            liq_box = {"left": d["PC_Liq_Left"], "top": d["PC_Liq_Top"], "right": d["PC_Liq_Right"], "bottom": d["PC_Liq_Bottom"]}
            profit_box = {"left": d["PC_Profit_Left"], "top": d["PC_Profit_Top"], "right": d["PC_Profit_Right"], "bottom": d["PC_Profit_Bottom"]}
        apr_box = {"left": crop_left, "top": crop_top, "right": crop_right, "bottom": crop_bottom}
        return {"TOTAL_LIQUIDITY": liq_box, "YESTERDAY_PROFIT": profit_box, "APR": apr_box}

    def _ocr_pc_metrics(
        self,
        file_bytes: bytes,
        crop_left: float, crop_top: float, crop_right: float, crop_bottom: float,
        srow: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Extract 3 metrics from a PC screenshot using 3 separate crop zones.

        The APR zone is the existing Crop_* region already in use.
        Liquidity and profit zones are configurable via Settings (PC_Liq_* / PC_Profit_*).
        Each zone retries once with an expanded box if no value is detected.
        """
        margin = AppConfig.OCR_EXPAND_MARGIN
        boxes = self._build_pc_boxes(srow, crop_left, crop_top, crop_right, crop_bottom)

        # PC画面がダークモード（黄色テキスト・暗背景）の場合は反転してOCR精度を向上
        file_bytes = U.maybe_invert_dark(file_bytes)

        liq_text = self._ocr_crop_text(file_bytes, boxes["TOTAL_LIQUIDITY"])
        profit_text = self._ocr_crop_text(file_bytes, boxes["YESTERDAY_PROFIT"])
        apr_text = self._ocr_crop_text(file_bytes, boxes["APR"])

        liq_vals = U.extract_usd_candidates(liq_text)
        profit_vals = U.extract_usd_candidates(profit_text)
        apr_vals = U.extract_percent_candidates(apr_text)

        # 流動性: 最大値（提供した流動性の合計）
        total_liquidity = U.pick_total_liquidity(liq_vals)
        # 昨日の収益: 1日前エントリ（末尾から2番目）の$ 値
        yesterday_profit = U.pick_prev_fee_amount(profit_vals)
        # 今日の手数料: 最新エントリ（末尾）の$ 値
        today_fee = U.pick_last_fee_amount(profit_vals)
        # 手数料合計: 全エントリ合計
        total_fees = U.sum_fee_amounts(profit_vals)
        apr_value = apr_vals[0] if apr_vals else None

        if total_liquidity is None:
            retry_text = self._ocr_crop_text(file_bytes, self._expand_box(boxes["TOTAL_LIQUIDITY"], margin))
            retry_vals = U.extract_usd_candidates(retry_text)
            total_liquidity = U.pick_total_liquidity(retry_vals) or total_liquidity
            if retry_vals:
                liq_text += f"\n[retry] {retry_text}"
                liq_vals = retry_vals

        if yesterday_profit is None:
            retry_text = self._ocr_crop_text(file_bytes, self._expand_box(boxes["YESTERDAY_PROFIT"], margin))
            retry_vals = U.extract_usd_candidates(retry_text)
            yesterday_profit = U.pick_prev_fee_amount(retry_vals) or yesterday_profit
            if not today_fee:
                today_fee = U.pick_last_fee_amount(retry_vals)
            total_fees = U.sum_fee_amounts(retry_vals) or total_fees
            if retry_vals:
                profit_text += f"\n[retry] {retry_text}"
                profit_vals = retry_vals

        if apr_value is None:
            retry_text = self._ocr_crop_text(file_bytes, self._expand_box(boxes["APR"], margin))
            retry_vals = U.extract_percent_candidates(retry_text)
            if retry_vals:
                apr_value = retry_vals[0]
                apr_text += f"\n[retry] {retry_text}"
                apr_vals = retry_vals

        boxed_preview = U.draw_ocr_boxes(file_bytes, boxes)

        # 操作履歴パネルの profit テキストから日時を抽出（PC 用）
        history_datetime = U.extract_history_datetime(profit_text)

        return {
            "boxes": boxes,
            "liq_text": liq_text,
            "profit_text": profit_text,
            "apr_text": apr_text,
            "liq_vals": liq_vals,
            "profit_vals": profit_vals,
            "apr_vals": apr_vals,
            "total_liquidity": total_liquidity,
            "yesterday_profit": yesterday_profit,   # 1日前エントリの$ 値
            "today_fee": today_fee,                  # 最新エントリの$ 値
            "total_fees": total_fees,                # 全手数料合計
            "apr_value": apr_value,
            "history_datetime": history_datetime,  # 最新手数料を回収の日時
            "boxed_preview": boxed_preview,
        }

    def _ocr_usdc_history(self, file_bytes: bytes, fast: bool = False) -> Dict[str, Any]:
        """OCR a full USDC transaction history screenshot and return all extracted rows.

        fast=True : 上半分のみ・反転なし・eng engine=1 の1コール（画面タイプ判定専用）
        fast=False: 全体・ダークモード反転あり・jpn→eng の順で精度重視
        """
        if fast:
            # 早期判定: 上半分だけ・反転なし・1コールのみ（小さい画像で高速）
            top_box = {"left": 0.0, "top": 0.05, "right": 1.0, "bottom": 0.50}
            raw_text = self._ocr_crop_text(file_bytes, top_box, language="eng", fast=True)
            rows = U.extract_transaction_rows(raw_text)
            return {"raw_text": raw_text, "rows": rows}

        # 確定読み取り: ダークモード対策で反転してから全体OCR
        ocr_bytes = U.maybe_invert_dark(file_bytes)
        full_box = {"left": 0.0, "top": 0.05, "right": 1.0, "bottom": 0.98}

        # First try: Japanese OCR — engine=1（jpn）で「月」を正確に読む
        raw_text = self._ocr_crop_text(ocr_bytes, full_box, language="jpn", fast=True)
        rows = U.extract_transaction_rows(raw_text)

        # Fallback: English OCR
        if not rows:
            raw_text_eng = self._ocr_crop_text(ocr_bytes, full_box, language="eng", fast=True)
            rows = U.extract_transaction_rows(raw_text_eng)
            if rows:
                raw_text = raw_text_eng

        return {"raw_text": raw_text, "rows": rows}

    def render_dashboard(self, members_df: pd.DataFrame, ledger_df: pd.DataFrame, apr_summary_df: pd.DataFrame) -> None:
        st.subheader("📊 管理画面ダッシュボード")
        st.caption("総資産 / 本日APR / グループ別残高 / 個人残高 / 個人別累計APR / LINE通知履歴")

        active_mem = members_df[members_df["IsActive"] == True].copy() if not members_df.empty else members_df.copy()
        total_assets = float(active_mem["Principal"].sum()) if not active_mem.empty else 0.0

        today_prefix, today_apr = U.fmt_date(U.now_jst()), 0.0
        if not ledger_df.empty and "Datetime_JST" in ledger_df.columns:
            today_rows = ledger_df[ledger_df["Datetime_JST"].astype(str).str.startswith(today_prefix)].copy()
            today_apr = float(today_rows[today_rows["Type"].astype(str).str.strip() == AppConfig.TYPE["APR"]]["Amount"].sum())

        c1, c2 = st.columns(2)
        c1.metric("総資産", U.fmt_usd(total_assets))
        c2.metric("本日APR", U.fmt_usd(today_apr))

        st.divider()
        c3, c4 = st.columns(2)

        with c3:
            st.markdown("#### グループ別残高")
            group_df = active_mem[active_mem["Project_Name"].astype(str).str.upper() != AppConfig.PROJECT["PERSONAL"]].copy() if not active_mem.empty else pd.DataFrame()
            if group_df.empty:
                st.info("グループデータがありません。")
            else:
                group_summary = group_df.groupby("Project_Name", as_index=False).agg(人数=("PersonName", "count"), 総残高=("Principal", "sum")).sort_values("総残高", ascending=False)
                group_summary["総残高"] = group_summary["総残高"].apply(U.fmt_usd)
                st.dataframe(group_summary, use_container_width=True, hide_index=True)

        with c4:
            st.markdown("#### 個人残高")
            personal_df = active_mem[active_mem["Project_Name"].astype(str).str.upper() == AppConfig.PROJECT["PERSONAL"]].copy() if not active_mem.empty else pd.DataFrame()
            if personal_df.empty:
                st.info("PERSONAL データがありません。")
            else:
                p = personal_df[["PersonName", "Principal", "LINE_DisplayName"]].copy()
                p["資産割合"] = p["Principal"].map(lambda x: f"{(float(x) / total_assets) * 100:.2f}%" if total_assets > 0 else "0.00%")
                p["Principal_num"] = p["Principal"].astype(float)
                p["Principal"] = p["Principal"].apply(U.fmt_usd)
                p = p.sort_values("Principal_num", ascending=False)[["PersonName", "Principal", "資産割合", "LINE_DisplayName"]]
                st.dataframe(p, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### 個人別 累計APR")
        if apr_summary_df.empty:
            st.info("APR履歴がありません。")
        else:
            view = apr_summary_df.copy()
            view["Total_APR_num"] = U.to_num_series(view["Total_APR"])
            view["Total_APR"] = view["Total_APR_num"].apply(U.fmt_usd)
            view = view.sort_values("Total_APR_num", ascending=False)[["PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"]]
            view = view.rename(columns={"Total_APR": "累計APR", "APR_Count": "件数", "Asset_Ratio": "総資産比"})
            st.dataframe(view, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### LINE通知履歴")
        c_hist1, c_hist2 = st.columns([1, 1])
        with c_hist1:
            if st.button("LINE送信履歴をリセット表示", use_container_width=True):
                st.session_state["hide_line_history"] = True
                st.rerun()
        with c_hist2:
            if st.button("LINE送信履歴を再表示", use_container_width=True):
                st.session_state["hide_line_history"] = False
                st.rerun()

        if st.session_state.get("hide_line_history", False):
            st.info("LINE通知履歴はリセット表示中です。シートの記録は削除していません。")
        else:
            if ledger_df.empty:
                st.info("通知履歴がありません。")
            else:
                line_hist = ledger_df[ledger_df["Type"].astype(str).str.strip() == AppConfig.TYPE["LINE"]].copy()
                if line_hist.empty:
                    st.info("LINE通知履歴はまだありません。")
                else:
                    cols = [c for c in ["Datetime_JST", "Project_Name", "PersonName", "Type", "Line_User_ID", "LINE_DisplayName", "Note", "Source"] if c in line_hist.columns]
                    st.dataframe(line_hist.sort_values("Datetime_JST", ascending=False)[cols].head(100), use_container_width=True, hide_index=True)

    def render_apr(self, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
        st.subheader("📈 APR 確定")
        st.caption(f"{AppConfig.RANK_LABEL} / PERSONAL=個別計算 / GROUP=総額均等割 / 管理者: {AdminAuth.current_label()}")

        # OCR結果をwidgetに反映（pendingキー経由）
        # ※ Streamlitはwidget初回作成後 value= 引数を無視するため、
        #   widget key に直接セットする必要があるが、それはwidget render前のみ可能。
        #   そのため OCR セクションでは _pending_input_sv_* に値を入れ、
        #   ここで widget key へ移し替えてから text_input を描画する。
        for _psrc, _wkey in [
            ("_pending_input_sv_liq",    "input_sv_liq"),
            ("_pending_input_sv_profit", "input_sv_profit"),
            ("_pending_input_sv_apr",    "input_sv_apr"),
        ]:
            if _psrc in st.session_state:
                st.session_state[_wkey] = st.session_state.pop(_psrc)

        # rerun後も保存結果を表示する（st.rerun()でメッセージが消えるのを防ぐ）
        if "_apr_save_success" in st.session_state:
            st.success(st.session_state.pop("_apr_save_success"))
            st.balloons()
        if "_apr_save_error" in st.session_state:
            st.error(st.session_state.pop("_apr_save_error"))

        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効（Active=TRUE）のプロジェクトがありません。")
            return

        project = st.selectbox("基準プロジェクト", projects)
        send_scope = st.radio("送信対象", ["選択中プロジェクトのみ", "全有効プロジェクト"], horizontal=True)

        st.markdown("#### 流動性 / 昨日の収益 / APR（別取得・手動設定可）")
        c1, c2, c3 = st.columns(3)
        with c1:
            total_liquidity_raw = st.text_input(
                "流動性（手動設定可）",
                value=st.session_state.get("sv_total_liquidity", ""),
                key="input_sv_liq",
                placeholder="$78,354.35",
            )
        with c2:
            yesterday_profit_raw = st.text_input(
                "昨日の収益（手動設定可）",
                value=st.session_state.get("sv_yesterday_profit", ""),
                key="input_sv_profit",
                placeholder="$90.87",
            )
        with c3:
            apr_raw = st.text_input(
                "APR（%・手動設定可）",
                value=st.session_state.get("sv_apr", ""),
                key="input_sv_apr",
                placeholder="42.33",
            )

        total_liquidity = U.to_f(total_liquidity_raw)
        yesterday_profit = U.to_f(yesterday_profit_raw)
        apr = U.apr_val(apr_raw)

        ocr_liquidity = st.session_state.get("ocr_total_liquidity")
        ocr_yesterday_profit = st.session_state.get("ocr_yesterday_profit")
        ocr_apr = st.session_state.get("ocr_apr")

        st.info(
            f"流動性 = {U.fmt_usd(total_liquidity)} / "
            f"昨日の収益 = {U.fmt_usd(yesterday_profit)} / "
            f"最終APR = {apr:.4f}%"
        )

        uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="apr_img")

        # Auto-run OCR when a new image is uploaded (detect change via file hash)
        _should_ocr = False
        if uploaded is not None:
            import hashlib
            _img_hash = hashlib.md5(uploaded.getvalue()).hexdigest()
            if st.session_state.get("_apr_img_last_hash") != _img_hash:
                st.session_state["_apr_img_last_hash"] = _img_hash
                _should_ocr = True
            # Also allow manual re-run
            if st.button("🔄 OCR再実行", key="apr_ocr_rerun"):
                _should_ocr = True

        if _should_ocr:
            file_bytes = uploaded.getvalue()
            is_mobile = U.is_mobile_tall_image(file_bytes)

            # Resolve crop ratios (APR zone for both PC and mobile)
            crop_left_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"]
            crop_top_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"]
            crop_right_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"]
            crop_bottom_ratio = AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"]
            srow_obj: Optional[Any] = None

            try:
                srow_obj = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
                if is_mobile:
                    crop_left_ratio = U.to_ratio(srow_obj.get("Crop_Left_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"]), AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"])
                    crop_top_ratio = U.to_ratio(srow_obj.get("Crop_Top_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"]), AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"])
                    crop_right_ratio = U.to_ratio(srow_obj.get("Crop_Right_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"]), AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"])
                    crop_bottom_ratio = U.to_ratio(srow_obj.get("Crop_Bottom_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"]), AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"])
                else:
                    crop_left_ratio = U.to_ratio(srow_obj.get("Crop_Left_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"]), AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"])
                    crop_top_ratio = U.to_ratio(srow_obj.get("Crop_Top_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"]), AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"])
                    crop_right_ratio = U.to_ratio(srow_obj.get("Crop_Right_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"]), AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"])
                    crop_bottom_ratio = U.to_ratio(srow_obj.get("Crop_Bottom_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"]), AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"])
            except Exception:
                pass

            img_type_label = "📱 モバイル" if is_mobile else "🖥️ PC"
            # ステップ2・3: 時刻取得とデバイス種別をsession_stateに保存
            st.session_state["_detected_device_type"] = "mobile" if is_mobile else "pc"
            st.session_state["_detected_device_time"] = U.fmt_dt(U.now_jst())
            st.caption(f"🔍 OCR実行中… 画像タイプ: {img_type_label}")

            _ocr_got_any = False  # Track if at least one value was detected
            _usdc_early: Optional[Dict[str, Any]] = None  # USDC先行検出結果

            with st.spinner("OCR処理中... しばらくお待ちください"):
                if is_mobile:
                    # ── モバイル: SmartVaultゾーンOCRを先に実行 ──
                    result = self._ocr_smartvault_mobile_metrics(file_bytes, srow=srow_obj)
                    liq_val    = result["total_liquidity"]
                    profit_val = result["yesterday_profit"]
                    apr_val    = result["apr_value"]
                    boxes      = result["boxes"]
                    texts      = {
                        "流動性":     result["total_text"],
                        "昨日の収益": result["profit_text"],
                        "APR":       result["apr_text"],
                    }
                else:
                    result = self._ocr_pc_metrics(
                        file_bytes,
                        crop_left_ratio, crop_top_ratio, crop_right_ratio, crop_bottom_ratio,
                        srow=srow_obj,
                    )
                    liq_val    = result["total_liquidity"]
                    profit_val = result["yesterday_profit"]
                    apr_val    = result["apr_value"]
                    boxes      = result["boxes"]
                    texts      = {
                        "流動性":     result["liq_text"],
                        "昨日の収益": result["profit_text"],
                        "APR":       result["apr_text"],
                    }
                    # PC: 操作履歴から抽出した日時・手数料合計をセッションに保存
                    _hist_dt = result.get("history_datetime")
                    if _hist_dt:
                        st.session_state["_pc_history_datetime"] = _hist_dt
                    st.session_state["_pc_total_fees"] = result.get("total_fees", 0.0)
                    st.session_state["_pc_today_fee"] = result.get("today_fee")

            # ── Show preview and results ──
            label_prefix = "📱 SmartVaultモバイル" if is_mobile else "🖥️ PC"
            st.markdown(f"#### {label_prefix} OCR結果")
            st.image(result["boxed_preview"], caption="赤枠 = OCR対象範囲", use_container_width=True)

            if not is_mobile:
                # PC: 4カラム表示（流動性・1日前手数料・今日手数料・手数料合計）
                _pc_today_f = result.get("today_fee") if result else None
                _pc_tot_f   = result.get("total_fees", 0.0) if result else 0.0
                _pc_dt_disp = st.session_state.get("_pc_history_datetime", "未検出")
                ca, cb, cc, cd = st.columns(4)
                with ca:
                    if liq_val is not None:
                        st.success(f"流動性合計\n{U.fmt_usd(float(liq_val))}")
                    else:
                        st.warning("流動性合計: 未検出")
                    st.caption("提供した流動性の合計")
                with cb:
                    if profit_val is not None:
                        st.success(f"昨日の収益\n{U.fmt_usd(float(profit_val))}")
                    else:
                        st.warning("昨日の収益: 未検出")
                    st.caption("手数料を回収（1日前）")
                with cc:
                    if _pc_today_f is not None:
                        st.success(f"今日の手数料\n{U.fmt_usd(float(_pc_today_f))}")
                    else:
                        st.warning("今日の手数料: 未検出")
                    st.caption("手数料を回収（最新）")
                with cd:
                    st.info(f"手数料合計\n{U.fmt_usd(float(_pc_tot_f))}")
                    st.caption(f"📅 {_pc_dt_disp}")
            else:
                # モバイル: 3カラム表示
                c_a, c_b, c_c = st.columns(3)
                with c_a:
                    if liq_val is not None:
                        st.success(f"流動性: {U.fmt_usd(float(liq_val))}")
                    else:
                        st.warning("流動性: 未検出")
                with c_b:
                    if profit_val is not None:
                        st.success(f"昨日の収益: {U.fmt_usd(float(profit_val))}")
                    else:
                        st.warning("昨日の収益: 未検出")
                with c_c:
                    if apr_val is not None:
                        st.success(f"APR: {float(apr_val):.4f}%")
                    else:
                        st.warning("APR: 未検出")

            # ── 使用座標を表で表示 ──
            b = boxes
            _all_none = liq_val is None and profit_val is None and apr_val is None
            with st.expander("使用中のOCR座標", expanded=_all_none):
                st.dataframe(
                    pd.DataFrame([
                        {"ゾーン": "流動性",     "Left": b["TOTAL_LIQUIDITY"]["left"],  "Top": b["TOTAL_LIQUIDITY"]["top"],  "Right": b["TOTAL_LIQUIDITY"]["right"],  "Bottom": b["TOTAL_LIQUIDITY"]["bottom"]},
                        {"ゾーン": "昨日の収益", "Left": b["YESTERDAY_PROFIT"]["left"], "Top": b["YESTERDAY_PROFIT"]["top"], "Right": b["YESTERDAY_PROFIT"]["right"], "Bottom": b["YESTERDAY_PROFIT"]["bottom"]},
                        {"ゾーン": "APR",        "Left": b["APR"]["left"],              "Top": b["APR"]["top"],              "Right": b["APR"]["right"],              "Bottom": b["APR"]["bottom"]},
                    ]),
                    hide_index=True,
                    use_container_width=True,
                )

            # ── OCR生テキスト（全未検出時は自動展開） ──
            for label, txt in texts.items():
                with st.expander(f"OCR生テキスト（{label}）", expanded=_all_none):
                    st.text(txt or "（テキスト取得なし）")

            # ── Store detected values into session_state ──
            if liq_val is not None:
                st.session_state["_pending_input_sv_liq"] = f"{float(liq_val):,.2f}"
                st.session_state["ocr_total_liquidity"] = float(liq_val)
                _ocr_got_any = True
            if profit_val is not None:
                st.session_state["_pending_input_sv_profit"] = f"{float(profit_val):,.2f}"
                st.session_state["ocr_yesterday_profit"] = float(profit_val)
                _ocr_got_any = True
            if apr_val is not None:
                st.session_state["_pending_input_sv_apr"] = f"{float(apr_val):.4f}"
                st.session_state["ocr_apr"] = float(apr_val)
                _ocr_got_any = True

            if _ocr_got_any:
                # Values stored → rerun so the text inputs and summary at the top refresh
                st.session_state.pop("_usdc_rows_cache", None)
                st.rerun()
            else:
                # ── USDC取引履歴フォールバック（モバイルかつ全値None のとき） ──
                if is_mobile:
                    # 先行USDC検出済みならAPI再呼び出し不要
                    if _usdc_early is not None:
                        usdc_result = _usdc_early
                        usdc_rows = usdc_result.get("rows", [])
                    else:
                        with st.spinner("USDC取引履歴として再読み取り中..."):
                            usdc_result = self._ocr_usdc_history(file_bytes)
                            usdc_rows = usdc_result.get("rows", [])
                    if usdc_rows:
                        # amount が None の行（金額未取得）を除外
                        usdc_rows = [r for r in usdc_rows if r.get("amount") is not None]
                    if usdc_rows:
                        # ボタンクリック時の再レンダリングでも参照できるようsession_stateに保存
                        total_amount = sum(r["amount"] for r in usdc_rows)
                        # 日付別に集計 → 最新日付=今日、その前日=昨日
                        from collections import defaultdict as _dd
                        _date_totals: dict = _dd(float)
                        for _ur in usdc_rows:
                            _date_totals[_ur["date_str"]] += _ur["amount"]
                        _sorted_dates = sorted(_date_totals.keys())
                        _most_recent_date = _sorted_dates[-1] if _sorted_dates else None
                        _prev_date = _sorted_dates[-2] if len(_sorted_dates) >= 2 else None
                        _today_usdc = _date_totals.get(_most_recent_date, 0.0) if _most_recent_date else 0.0
                        _yest_usdc = _date_totals.get(_prev_date, 0.0) if _prev_date else 0.0
                        # session_state に保存（ボタンクリック後の再レンダリングでも参照できるようにする）
                        st.session_state["_usdc_rows_cache"] = usdc_rows
                        st.session_state["_usdc_project_cache"] = str(project)
                        st.session_state["_usdc_raw_text_cache"] = usdc_result.get("raw_text", "")
                        st.session_state["_usdc_total_cache"] = total_amount
                        st.session_state["_today_usdc_total"] = _today_usdc
                        # 昨日の収益 = 最新日付より前日のUSDC合計
                        st.session_state["_pending_input_sv_profit"] = f"{_yest_usdc:,.2f}"
                        st.session_state["ocr_yesterday_profit"] = _yest_usdc
                        try:
                            _srow = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
                            _factor = float(_srow.get("Net_Factor", AppConfig.FACTOR["MASTER"]))
                            if _factor <= 0:
                                _factor = float(AppConfig.FACTOR["MASTER"])
                            _mem_active = self.repo.project_members_active(members_df, project)
                            _total_principal = float(_mem_active["Principal"].sum()) if not _mem_active.empty else 0.0
                            if _total_principal > 0 and _factor > 0:
                                # APR%は今日のUSDC合計から算出
                                _auto_apr = (_today_usdc / (_total_principal * _factor)) * 365.0 * 100.0
                                st.session_state["_pending_input_sv_apr"] = f"{_auto_apr:.4f}"
                                st.session_state["ocr_apr"] = _auto_apr
                        except Exception:
                            pass  # APR自動計算失敗時は手動入力にフォールバック
                        st.rerun()
                    else:
                        st.error(
                            "⚠️ OCRで数値を検出できませんでした（SmartVault / USDC取引履歴ともに未検出）。\n\n"
                            "**考えられる原因と対処法:**\n"
                            "1. 赤枠の位置がずれている → ❓ ヘルプ ページの「OCR設定（座標設定）」で座標を調整してください。\n"
                            "2. 画像が低解像度または小さすぎる → 元のスクリーンショットを使用してください。\n"
                            "3. 数値が画面に表示されていない → 正しいページのスクショか確認してください。\n\n"
                            "値が読み取れない場合は上の入力欄に手動で入力してください。"
                        )
                else:
                    # PC画像で未検出
                    st.error(
                        "⚠️ OCRで数値を検出できませんでした。\n\n"
                        "**考えられる原因と対処法:**\n"
                        "1. 赤枠の位置がずれている → ❓ ヘルプ ページの「OCR設定（座標設定）」で座標を調整してください。\n"
                        "2. 画像が低解像度または小さすぎる → 元のスクリーンショットを使用してください。\n"
                        "3. 数値が画面に表示されていない → 正しいページのスクショか確認してください。\n\n"
                        "値が読み取れない場合は上の入力欄に手動で入力してください。"
                    )

        # ── USDC OCR結果の表示＋保存ボタン（_should_ocr 外：rerun後も表示継続） ──
        _cached_usdc_rows = st.session_state.get("_usdc_rows_cache", [])
        if _cached_usdc_rows:
            _cached_total = st.session_state.get("_usdc_total_cache", sum(r["amount"] for r in _cached_usdc_rows))
            st.info(
                f"📋 **USDC取引履歴** を検出しました（{len(_cached_usdc_rows)} 件）。"
                f" 合計: **{U.fmt_usd(_cached_total)}** → 昨日の収益・APR% に自動セット済み"
            )
            _cached_rows_df = pd.DataFrame([
                {"日付": r["date_str"], "時刻": r["time_str"], "金額($)": r["amount"]}
                for r in _cached_usdc_rows
            ])
            st.dataframe(_cached_rows_df, use_container_width=True, hide_index=True)
            with st.expander("OCR生テキスト（USDC履歴）", expanded=False):
                st.text(st.session_state.get("_usdc_raw_text_cache") or "（テキスト取得なし）")
            if st.button(
                "📊 USDC履歴をシートに保存",
                key="usdc_save_to_sheet",
                use_container_width=True,
            ):
                try:
                    _save_project = st.session_state.get("_usdc_project_cache", str(project))
                    written, skipped = self.repo.append_usdc_history_rows(
                        rows=_cached_usdc_rows,
                        project=_save_project,
                        admin_name=AdminAuth.current_label(),
                        admin_namespace=AdminAuth.current_namespace(),
                    )
                    if written > 0 and skipped == 0:
                        st.success(f"✅ {written} 件を USDC_History シートに保存しました。")
                    elif written > 0:
                        st.success(f"✅ {written} 件保存しました（{skipped} 件は重複のためスキップ）。")
                    else:
                        st.info(f"ℹ️ 全 {skipped} 件は既にシートに存在するためスキップしました。")
                except Exception as _e:
                    st.error(f"保存エラー: {_e}")

        target_projects = projects if send_scope == "全有効プロジェクト" else [project]
        today_key = U.fmt_date(U.now_jst())
        existing_apr_keys = self.repo.existing_apr_keys_for_date(today_key)

        # ── ステップ4・5・6・7: 本日の積み上げデータ＋合算APR ──────────────
        st.markdown("---")
        st.markdown("#### 📋 本日の積み上げデータ")
        today_sv_df = self.repo.load_today_smartvault_history(today_key, project)

        _detected_device = st.session_state.get("_detected_device_type", "")

        if not today_sv_df.empty:
            # ステップ4: 重複チェック（PC/携帯横断）
            if _detected_device and "Device_Type" in today_sv_df.columns:
                same_device_df = today_sv_df[
                    today_sv_df["Device_Type"].astype(str).str.strip().str.lower()
                    == _detected_device.lower()
                ]
                if not same_device_df.empty:
                    _dev_label = "📱 モバイル" if _detected_device == "mobile" else "🖥️ PC"
                    st.warning(
                        f"⚠️ 本日すでに {_dev_label} データが {len(same_device_df)} 件記録されています。"
                        " 同一デバイスの重複登録になります。"
                    )

            # ステップ5: 日次積み上げ表示
            _disp_cols = [c for c in ["Datetime_JST", "Device_Type", "APR", "Liquidity", "Yesterday_Profit", "Source_Mode"] if c in today_sv_df.columns]
            st.dataframe(today_sv_df[_disp_cols].reset_index(drop=True), use_container_width=True, hide_index=True)

            # ステップ6・7: PCだけ66% ／ 合算APR 計算
            _combined_apr, _explanation = U.calc_combined_apr(today_sv_df)
            if _combined_apr is not None:
                _dev_now = st.session_state.get("_detected_device_type", "")
                if _dev_now == "pc":
                    # PC: 手数料合計$＋最終日時を表示
                    _comb_fees = st.session_state.get("_pc_total_fees", 0.0)
                    _comb_dt   = st.session_state.get("_pc_history_datetime", "")
                    _comb_yest = st.session_state.get("ocr_yesterday_profit", 0.0) or 0.0
                    _comb_today_f = st.session_state.get("_pc_today_fee")
                    _comb_info = (
                        f"🖥️ **PC手数料** — 1日前: **{U.fmt_usd(_comb_yest)}**"
                        + (f" ／ 今日: **{U.fmt_usd(float(_comb_today_f))}**" if _comb_today_f else "")
                        + f" ／ 合計: **{U.fmt_usd(_comb_fees)}**"
                        + (f" ／ 日時: {_comb_dt}" if _comb_dt else "")
                    )
                    st.info(_comb_info)
                    st.caption(f"APR算出参考値: {_combined_apr:.4f}% — {_explanation}")
                else:
                    st.info(f"🔢 **合算APR: {_combined_apr:.4f}%** — {_explanation}")
                if st.button("⬆️ 合算APRをフォームに反映", key="use_combined_apr_btn"):
                    st.session_state["_pending_input_sv_apr"] = f"{_combined_apr:.4f}"
                    st.rerun()
        else:
            st.caption("（本日のSmartVaultデータはまだありません。APR確定時に記録されます）")
        st.markdown("---")

        preview_rows: List[dict] = []
        total_members, total_principal, total_reward, skipped_members = 0, 0.0, 0.0, 0

        for p in target_projects:
            row = settings_df[settings_df["Project_Name"] == str(p)].iloc[0]
            project_net_factor = float(row.get("Net_Factor", AppConfig.FACTOR["MASTER"]))
            compound_timing = U.normalize_compound(row.get("Compound_Timing", AppConfig.COMPOUND["NONE"]))
            mem = self.repo.project_members_active(members_df, p)
            if mem.empty:
                continue

            mem_calc = self.engine.calc_project_apr(mem, float(apr), project_net_factor, p)
            for _, r in mem_calc.iterrows():
                person = str(r["PersonName"]).strip()
                is_done = (str(p).strip(), person) in existing_apr_keys
                if is_done:
                    skipped_members += 1
                else:
                    total_members += 1
                    total_principal += float(r["Principal"])
                    total_reward += float(r["DailyAPR"])

                preview_rows.append(
                    {
                        "Project_Name": p,
                        "PersonName": person,
                        "Rank": str(r["Rank"]).strip(),
                        "Compound_Timing": U.compound_label(compound_timing),
                        "Principal": U.fmt_usd(float(r["Principal"])),
                        "DailyAPR": U.fmt_usd(float(r["DailyAPR"])),
                        "Line_User_ID": str(r["Line_User_ID"]).strip(),
                        "LINE_DisplayName": str(r["LINE_DisplayName"]).strip(),
                        "流動性": U.fmt_usd(float(total_liquidity)),
                        "昨日の収益": U.fmt_usd(float(yesterday_profit)),
                        "APR": f"{apr:.4f}%",
                        "本日APR状態": "本日記録済み" if is_done else "未記録",
                    }
                )

        if total_members == 0 and skipped_members == 0:
            st.warning("送信対象に 🟢運用中 のメンバーがいません。")
            return

        st.markdown(f"送信対象プロジェクト数: {len(target_projects)} / 本日未記録の対象人数: {total_members} / 本日記録済み人数: {skipped_members}")

        apr_percent_display = (total_reward / total_principal * 100.0) if total_principal > 0 else 0.0

        csum1, csum2 = st.columns([1.2, 2.8])
        with csum1:
            if send_scope == "選択中プロジェクトのみ":
                if st.button("本日のAPR記録をリセット", key="reset_today_apr_top", use_container_width=True):
                    try:
                        deleted_apr, deleted_line = self.repo.reset_today_apr_records(today_key, project)
                        self.store.persist_and_refresh()
                        if deleted_apr == 0 and deleted_line == 0:
                            st.info("削除対象はありません。")
                        else:
                            st.success(f"本日分をリセットしました。APR削除:{deleted_apr}件 / LINE削除:{deleted_line}件")
                        st.rerun()
                    except Exception as e:
                        st.error(f"APRリセットでエラー: {e}")
                        st.stop()

        with csum2:
            # 画像タイプ別にサマリー表示を切り替え
            _sum_dev = st.session_state.get("_detected_device_type", "")
            _sum_is_usdc = bool(st.session_state.get("_usdc_rows_cache"))
            _sum_dt_label = st.session_state.get("_detected_device_time", "")
            if _sum_dev == "pc":
                # PC: APR合計=手数料合計 / 今日のAPR=最終エントリ日時 / 実効APR=66%固定
                _sum_total_fees = st.session_state.get("_pc_total_fees", 0.0)
                _sum_hist_dt = st.session_state.get("_pc_history_datetime", "未検出")
                _sum_today_f = st.session_state.get("_pc_today_fee")
                _sum_device_label = f"🖥️ PC画像 ({_sum_dt_label})"
                _sum_line1_right = f"APR合計: **{U.fmt_usd(_sum_total_fees)}**"
                _sum_line2_mid   = f"今日のAPR: **{_sum_hist_dt}**"
                _sum_line2_right = "実効APR: **66.0000%**"
                # PC追加行: 1日前と今日の手数料を明示
                _pc_extra = (
                    f"手数料（1日前）: **{U.fmt_usd(float(yesterday_profit or 0))}**"
                    + (f"　/　手数料（今日）: **{U.fmt_usd(float(_sum_today_f))}**" if _sum_today_f else "")
                )
            elif _sum_is_usdc:
                # モバイル USDC: APR合計=今日USDC合計 / 実効APR通常表示
                _sum_today_usdc = st.session_state.get("_today_usdc_total", 0.0)
                _sum_device_label = f"📱 モバイル USDC ({_sum_dt_label})"
                _sum_line1_right = f"APR合計: **{U.fmt_usd(_sum_today_usdc)}**"
                _sum_line2_mid   = f"APR合計: **{U.fmt_usd(total_reward)}**"
                _sum_line2_right = f"実効APR: **{apr_percent_display:.4f}%**"
                _pc_extra = ""
            else:
                # SmartVault モバイル: 従来表示
                _sum_device_label = f"📱 SmartVaultモバイル ({_sum_dt_label})" if _sum_dev == "mobile" else ""
                _sum_line1_right = f"最終APR: **{apr:.4f}%**"
                _sum_line2_mid   = f"APR合計: **{U.fmt_usd(total_reward)}**"
                _sum_line2_right = f"実効APR: **{apr_percent_display:.4f}%**"
                _pc_extra = ""
            _extra_line = f"\n{_pc_extra}" if _pc_extra else ""
            _device_line = f"\n*{_sum_device_label}*" if _sum_device_label else ""
            st.markdown(
                f"""
**本日対象サマリー**{_device_line}
流動性: **{U.fmt_usd(total_liquidity)}**　/　昨日の収益: **{U.fmt_usd(yesterday_profit)}**　/　{_sum_line1_right}{_extra_line}
総投資額: **{U.fmt_usd(total_principal)}**　/　{_sum_line2_mid}　/　{_sum_line2_right}
"""
            )

        with st.expander("個人別の本日配当（確認）", expanded=False):
            st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)

        # 保存前デバッグ情報
        with st.expander("🔍 保存前デバッグ情報（問題診断用）", expanded=True):
            st.write(f"apr={apr}, yesterday_profit={yesterday_profit}, total_liquidity={total_liquidity}")
            st.write(f"total_members={total_members}, skip={skipped_members}, target_projects={target_projects}")
            st.write(f"input_sv_apr={st.session_state.get('input_sv_apr','')}, input_sv_profit={st.session_state.get('input_sv_profit','')}")
            st.write(f"Ledger sheet: {self.repo.gs.names.LEDGER} / namespace: '{AdminAuth.current_namespace()}'")
            st.write(f"SmartVault_History sheet: {self.repo.gs.names.SMARTVAULT_HISTORY}")
            if "_sv_write_ok" in st.session_state:
                st.success(f"✅ SmartVault 前回書き込み: {st.session_state.pop('_sv_write_ok')}")
            if "_sv_write_err" in st.session_state:
                st.error(f"❌ SmartVault 前回書き込みエラー: {st.session_state.pop('_sv_write_err')}")

        if st.button("APRを確定して対象全員にLINE送信", key="apr_confirm_btn"):
            _save_warnings: List[str] = []
            try:
                if apr <= 0:
                    st.warning(f"⚠️ APRが0以下です（apr={apr}）。画像を再アップロードするか手動でAPRを入力してください。")
                    return

                evidence_url = ""
                if uploaded:
                    import mimetypes as _mt
                    _fname = getattr(uploaded, "name", "evidence.jpg")
                    _mime = _mt.guess_type(_fname)[0] or "image/jpeg"

                    # ① Google Drive にアップロード（サービスアカウントで権限不要）
                    _drive_url = self.repo.gs.upload_image_to_drive(
                        uploaded.getvalue(), _fname, _mime
                    )
                    if _drive_url:
                        # =IMAGE("url") 式でセルに画像を直接表示
                        evidence_url = f'=IMAGE("{_drive_url}")'
                    else:
                        # ② フォールバック: ImgBB
                        _imgbb_url = ExternalService.upload_imgbb(uploaded.getvalue())
                        if _imgbb_url:
                            evidence_url = _imgbb_url
                        else:
                            _save_warnings.append("画像アップロード失敗（Drive / ImgBB ともに失敗）。エビデンスなしで保存します。")

                source_mode = U.detect_source_mode(
                    final_liquidity=float(total_liquidity),
                    final_profit=float(yesterday_profit),
                    final_apr=float(apr),
                    ocr_liquidity=st.session_state.get("ocr_total_liquidity"),
                    ocr_profit=st.session_state.get("ocr_yesterday_profit"),
                    ocr_apr=st.session_state.get("ocr_apr"),
                )
                # ステップ3: デバイス種別取得（OCR時に保存済み）
                _device_type = st.session_state.get("_detected_device_type", "")

                ts = U.fmt_dt(U.now_jst())
                apr_ledger_count, line_log_count, success, fail, skip_count = 0, 0, 0, 0, 0
                existing_apr_keys = self.repo.existing_apr_keys_for_date(today_key)
                daily_add_map: Dict[Tuple[str, str], float] = {}

                # SmartVault 履歴を記録（個別エラーを捕捉して可視化）
                try:
                    self.repo.append_smartvault_history(
                        dt_jst=ts,
                        project=project,
                        liquidity=float(total_liquidity),
                        yesterday_profit=float(yesterday_profit),
                        apr=float(apr),
                        source_mode=source_mode,
                        ocr_liquidity=st.session_state.get("ocr_total_liquidity"),
                        ocr_yesterday_profit=st.session_state.get("ocr_yesterday_profit"),
                        ocr_apr=st.session_state.get("ocr_apr"),
                        evidence_url=evidence_url or "",
                        admin_name=AdminAuth.current_name(),
                        admin_namespace=AdminAuth.current_namespace(),
                        note="APR確定時に保存",
                        device_type=_device_type,  # ステップ3: pc / mobile
                    )
                    st.session_state["_sv_write_ok"] = f"{ts} / device={_device_type or '(未設定)'}"
                except Exception as _sv_err:
                    import traceback as _sv_tb
                    st.session_state["_sv_write_err"] = f"{_sv_err}\n{_sv_tb.format_exc()[:600]}"
                    _save_warnings.append(f"SmartVault_History 書き込みエラー: {_sv_err}")

                # LINE トークン取得（失敗しても Ledger 書き込みは続ける）
                token: Optional[str] = None
                try:
                    token = ExternalService.get_line_token(AdminAuth.current_namespace())
                except Exception as _tok_e:
                    _save_warnings.append(f"LINEトークン取得エラー（LINE送信をスキップ）: {_tok_e}")

                # Ledger 書き込み & LINE 送信
                for p in target_projects:
                    _proj_row = settings_df[settings_df["Project_Name"] == str(p)].iloc[0]
                    project_net_factor = float(_proj_row.get("Net_Factor", AppConfig.FACTOR["MASTER"]))
                    compound_timing = U.normalize_compound(_proj_row.get("Compound_Timing", AppConfig.COMPOUND["NONE"]))
                    mem = self.repo.project_members_active(members_df, p)
                    if mem.empty:
                        continue

                    mem_calc = self.engine.calc_project_apr(mem, float(apr), project_net_factor, p)
                    for _, r in mem_calc.iterrows():
                        person = str(r["PersonName"]).strip()
                        uid = str(r["Line_User_ID"]).strip()
                        disp = str(r["LINE_DisplayName"]).strip()
                        daily_apr = float(r["DailyAPR"])
                        current_principal = float(r["Principal"])
                        apr_key = (str(p).strip(), person)

                        if apr_key in existing_apr_keys:
                            skip_count += 1
                            continue

                        note = (
                            f"APR:{apr}%, "
                            f"Liquidity:{total_liquidity}, "
                            f"YesterdayProfit:{yesterday_profit}, "
                            f"SourceMode:{source_mode}, "
                            f"Mode:{r['CalcMode']}, Rank:{r['Rank']}, Factor:{r['Factor']}, CompoundTiming:{compound_timing}"
                        )
                        self.repo.append_ledger(ts, p, person, AppConfig.TYPE["APR"], daily_apr, note, evidence_url or "", uid, disp)
                        existing_apr_keys.add(apr_key)
                        apr_ledger_count += 1

                        if compound_timing == AppConfig.COMPOUND["DAILY"]:
                            daily_add_map[(str(p).strip(), person)] = daily_add_map.get((str(p).strip(), person), 0.0) + daily_apr
                            person_after_amount = current_principal + daily_apr
                        else:
                            person_after_amount = current_principal

                        personalized_msg = (
                            "🏦【APR収益報告】\n"
                            f"{person} 様\n"
                            f"報告日時: {U.now_jst().strftime('%Y/%m/%d %H:%M')}\n"
                            f"流動性: {U.fmt_usd(total_liquidity)}\n"
                            f"昨日の収益: {U.fmt_usd(yesterday_profit)}\n"
                            f"APR: {apr:.4f}%\n"
                            f"本日配当: {U.fmt_usd(daily_apr)}\n"
                            f"現在運用額: {U.fmt_usd(current_principal)}\n"
                            f"複利タイプ: {U.compound_label(compound_timing)}\n"
                        )

                        if compound_timing == AppConfig.COMPOUND["DAILY"]:
                            personalized_msg += f"複利反映後運用額: {U.fmt_usd(person_after_amount)}\n"

                        if not uid or token is None:
                            code, line_note = 0, "LINE未送信: Line_User_IDなしまたはトークン未取得"
                        else:
                            code = ExternalService.send_line_push(token, uid, personalized_msg, evidence_url)
                            line_note = (
                                f"HTTP:{code}, "
                                f"Liquidity:{total_liquidity}, "
                                f"YesterdayProfit:{yesterday_profit}, "
                                f"APR:{apr}%, SourceMode:{source_mode}, CompoundTiming:{compound_timing}"
                            )

                        self.repo.append_ledger(ts, p, person, AppConfig.TYPE["LINE"], 0, line_note, evidence_url or "", uid, disp)
                        line_log_count += 1

                        if code == 200:
                            success += 1
                        else:
                            fail += 1

                if daily_add_map:
                    for _di in range(len(members_df)):
                        _dp = str(members_df.loc[_di, "Project_Name"]).strip()
                        _dpn = str(members_df.loc[_di, "PersonName"]).strip()
                        _addv = float(daily_add_map.get((_dp, _dpn), 0.0))
                        if _addv != 0.0 and U.truthy(members_df.loc[_di, "IsActive"]):
                            members_df.loc[_di, "Principal"] = float(members_df.loc[_di, "Principal"]) + _addv
                            members_df.loc[_di, "UpdatedAt_JST"] = ts
                    self.repo.write_members(members_df)

                self.store.persist_and_refresh()
                _success_msg = (
                    f"✅ APR記録:{apr_ledger_count}件 / LINE履歴記録:{line_log_count}件 / "
                    f"送信成功:{success} / 送信失敗:{fail} / 重複スキップ:{skip_count}件"
                )
                if _save_warnings:
                    _success_msg += " ⚠️ " + " / ".join(_save_warnings)
                st.session_state["_apr_save_success"] = _success_msg
                st.rerun()

            except Exception as e:
                import traceback as _tb
                _err_detail = _tb.format_exc()
                st.error(f"APR確定処理でエラー: {e}\n詳細: {_err_detail[:800]}")
                st.session_state["_apr_save_error"] = f"APR確定処理でエラー: {e}"

        if send_scope == "選択中プロジェクトのみ":
            row = settings_df[settings_df["Project_Name"] == str(project)].iloc[0]
            compound_timing = U.normalize_compound(row.get("Compound_Timing", AppConfig.COMPOUND["NONE"]))
            if compound_timing == AppConfig.COMPOUND["MONTHLY"]:
                st.divider()
                st.markdown("#### 月次複利反映")
                if st.button("未反映APRを元本へ反映"):
                    try:
                        count, total_added = self.engine.apply_monthly_compound(self.repo, members_df, project)
                        self.store.persist_and_refresh()
                        if count == 0:
                            st.info("未反映のAPRはありません。")
                        else:
                            st.success(f"{count}名に反映しました。合計反映額: {U.fmt_usd(total_added)}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"月次複利反映でエラー: {e}")
                        st.stop()

    def render_cash(self, settings_df: pd.DataFrame, members_df: pd.DataFrame) -> None:
        st.subheader("💸 入金 / 出金（個別LINE通知）")
        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効なプロジェクトがありません。")
            return

        # ── USDC Transaction History OCR ──────────────────────────────────────
        with st.expander("📱 USDCトランザクション履歴 OCR読み取り（スマートフォン画面）", expanded=False):
            st.caption("Coinbase等のUSDC受け取り履歴画面をアップロードすると、日付・時刻・金額を自動で一覧抽出します。")
            hist_img = st.file_uploader(
                "USDC履歴画像をアップロード",
                type=["png", "jpg", "jpeg"],
                key="cash_usdc_hist",
            )
            if hist_img is not None and st.button("OCR読み取り開始", key="cash_usdc_ocr_btn"):
                with st.spinner("OCR実行中..."):
                    hist_result = self._ocr_usdc_history(hist_img.getvalue())

                st.markdown("#### 抽出結果")
                rows = hist_result["rows"]
                if rows:
                    df_rows = pd.DataFrame(
                        [
                            {
                                "日時(JST)": r["datetime_jst"],
                                "日付": r["date_str"],
                                "時刻": r["time_str"],
                                "金額($)": r["amount"] if r["amount"] is not None else "未検出",
                            }
                            for r in rows
                        ]
                    )
                    st.dataframe(df_rows, use_container_width=True, hide_index=True)
                    total = sum(r["amount"] for r in rows if r["amount"] is not None)
                    st.success(f"合計 {len(rows)} 件 / 合計金額: {U.fmt_usd(total)}")
                else:
                    st.warning("トランザクション行が検出されませんでした。画像を確認してください。")
                    with st.expander("OCR生テキスト", expanded=True):
                        st.text(hist_result["raw_text"] or "（テキスト取得なし）")

        st.divider()
        # ── Manual cash entry ─────────────────────────────────────────────────
        project = st.selectbox("プロジェクト", projects, key="cash_project")
        mem = self.repo.project_members_active(members_df, project)
        if mem.empty:
            st.warning("このプロジェクトに 🟢運用中 のメンバーがいません。")
            return

        person = st.selectbox("メンバー", mem["PersonName"].tolist())
        row = mem[mem["PersonName"] == person].iloc[0]
        current = float(row["Principal"])

        typ = st.selectbox("種別", [AppConfig.TYPE["DEPOSIT"], AppConfig.TYPE["WITHDRAW"]])
        amt = st.number_input("金額", min_value=0.0, value=0.0, step=100.0)
        note = st.text_input("メモ（任意）", value="")
        uploaded = st.file_uploader("エビデンス画像（任意）", type=["png", "jpg", "jpeg"], key="cash_img")

        if st.button("確定して保存＆個別にLINE通知"):
            try:
                if amt <= 0:
                    st.warning("金額が0です。")
                    return
                if typ == AppConfig.TYPE["WITHDRAW"] and float(amt) > current:
                    st.error("出金額が現在残高を超えています。")
                    return

                evidence_url = ExternalService.upload_imgbb(uploaded.getvalue()) if uploaded else None
                if uploaded and not evidence_url:
                    st.error("画像アップロードに失敗しました。")
                    return

                new_balance = current + float(amt) if typ == AppConfig.TYPE["DEPOSIT"] else current - float(amt)
                ts = U.fmt_dt(U.now_jst())

                for i in range(len(members_df)):
                    if members_df.loc[i, "Project_Name"] == str(project) and str(members_df.loc[i, "PersonName"]).strip() == str(person).strip():
                        members_df.loc[i, "Principal"] = float(new_balance)
                        members_df.loc[i, "UpdatedAt_JST"] = ts

                self.repo.append_ledger(
                    ts,
                    project,
                    person,
                    typ,
                    float(amt),
                    note,
                    evidence_url or "",
                    str(row["Line_User_ID"]).strip(),
                    str(row["LINE_DisplayName"]).strip(),
                )
                self.repo.write_members(members_df)

                token = ExternalService.get_line_token(AdminAuth.current_namespace())
                uid = str(row["Line_User_ID"]).strip()
                msg = (
                    "💸【入出金通知】\n"
                    f"{person} 様\n"
                    f"日時: {U.now_jst().strftime('%Y/%m/%d %H:%M')}\n"
                    f"種別: {typ}\n"
                    f"金額: {U.fmt_usd(float(amt))}\n"
                    f"更新後残高: {U.fmt_usd(float(new_balance))}\n"
                )

                if uid:
                    code = ExternalService.send_line_push(token, uid, msg, evidence_url)
                    line_note = f"HTTP:{code}, Type:{typ}, Amount:{float(amt)}, NewBalance:{float(new_balance)}"
                else:
                    code, line_note = 0, "LINE未送信: Line_User_IDなし"

                self.repo.append_ledger(ts, project, person, AppConfig.TYPE["LINE"], 0, line_note, evidence_url or "", uid, str(row["LINE_DisplayName"]).strip())
                self.store.persist_and_refresh()

                if code == 200:
                    st.success("入出金保存＆LINE送信記録完了")
                else:
                    st.warning(f"入出金保存完了 / LINE送信または送信記録あり（HTTP {code}）")
                st.rerun()
            except Exception as e:
                st.error(f"入出金処理でエラー: {e}")
                st.stop()

    def render_admin(self, settings_df: pd.DataFrame, members_df: pd.DataFrame, line_users_df: pd.DataFrame) -> None:
        st.subheader("⚙️ 管理")

        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効なプロジェクトがありません。")
            return

        project = st.selectbox("対象プロジェクト", projects, key="admin_project")

        line_users: List[Tuple[str, str, str]] = []
        if not line_users_df.empty:
            tmp = line_users_df[line_users_df["Line_User_ID"].astype(str).str.startswith("U")].drop_duplicates(subset=["Line_User_ID"], keep="last")
            for _, r in tmp.iterrows():
                uid = str(r["Line_User_ID"]).strip()
                name = str(r.get("Line_User", "")).strip()
                line_users.append((f"{name} ({uid})" if name else uid, uid, name))

        view_all = members_df[members_df["Project_Name"] == str(project)].copy()
        view_all["_row_id"] = view_all.index

        if not view_all.empty:
            st.markdown("#### 現在のメンバー一覧")
            show = view_all.copy()
            show["Principal"] = show["Principal"].apply(U.fmt_usd)
            show["状態"] = show["IsActive"].apply(U.bool_to_status)
            st.dataframe(show.drop(columns=["_row_id"], errors="ignore"), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### 📨 メンバーから選択して個別にLINE送信（個人名 自動挿入）")
        if view_all.empty:
            st.info("メンバーがいないため送信できません。")
        else:
            target_mode = st.radio("対象", ["🟢運用中のみ", "全メンバー（停止含む）"], horizontal=True)
            cand = view_all.copy() if target_mode.startswith("全") else view_all[view_all["IsActive"] == True].copy().reset_index(drop=True)

            def label_row(r: pd.Series) -> str:
                name = str(r.get("PersonName", "")).strip()
                disp = str(r.get("LINE_DisplayName", "")).strip()
                uid = str(r.get("Line_User_ID", "")).strip()
                stt = U.bool_to_status(r.get("IsActive", True))
                return f"{stt} {name} / {disp}" if disp else f"{stt} {name} / {uid}"

            options = [label_row(cand.loc[i]) for i in range(len(cand))]
            selected = st.multiselect("送信先（複数可）", options=options)

            default_msg = f"【ご連絡】\nプロジェクト: {project}\n日時: {U.now_jst().strftime('%Y/%m/%d %H:%M')}\n\n"
            msg_common = st.text_area(
                "メッセージ本文（共通）※送信時に「〇〇 様」を自動挿入します",
                value=st.session_state.get("direct_line_msg", default_msg),
                height=180,
            )
            st.session_state["direct_line_msg"] = msg_common
            img = st.file_uploader("添付画像（任意・ImgBB）", type=["png", "jpg", "jpeg"], key="direct_line_img")

            c1, c2 = st.columns([1, 1])
            do_send = c1.button("選択メンバーへ送信", use_container_width=True)
            clear_msg = c2.button("本文を初期化", use_container_width=True)

            if clear_msg:
                st.session_state["direct_line_msg"] = default_msg
                st.rerun()

            if do_send:
                if not selected:
                    st.warning("送信先を選択してください。")
                elif not msg_common.strip():
                    st.warning("メッセージが空です。")
                else:
                    evidence_url = ExternalService.upload_imgbb(img.getvalue()) if img else None
                    if img and not evidence_url:
                        st.error("画像アップロードに失敗しました。")
                        return

                    token = ExternalService.get_line_token(AdminAuth.current_namespace())
                    label_to_row = {label_row(cand.loc[i]): cand.loc[i] for i in range(len(cand))}
                    success, fail, failed_list, ts, line_log_count = 0, 0, [], U.fmt_dt(U.now_jst()), 0

                    for lab in selected:
                        r = label_to_row.get(lab)
                        if r is None:
                            fail += 1
                            failed_list.append(lab)
                            continue

                        uid = str(r.get("Line_User_ID", "")).strip()
                        person_name = str(r.get("PersonName", "")).strip()
                        disp = str(r.get("LINE_DisplayName", "")).strip()
                        personalized = U.insert_person_name(msg_common, person_name)

                        if not U.is_line_uid(uid):
                            fail += 1
                            failed_list.append(f"{lab}（Line_User_ID不正）")
                            self.repo.append_ledger(ts, project, person_name, AppConfig.TYPE["LINE"], 0, "LINE未送信: Line_User_ID不正", evidence_url or "", uid, disp)
                            line_log_count += 1
                            continue

                        code = ExternalService.send_line_push(token, uid, personalized, evidence_url)
                        self.repo.append_ledger(ts, project, person_name, AppConfig.TYPE["LINE"], 0, f"HTTP:{code}, DirectMessage", evidence_url or "", uid, disp)
                        line_log_count += 1

                        if code == 200:
                            success += 1
                        else:
                            fail += 1
                            failed_list.append(f"{lab}（HTTP {code}）")

                    self.store.persist_and_refresh()
                    if fail == 0:
                        st.success(f"送信完了（成功:{success} / 失敗:{fail} / Ledger記録:{line_log_count}）")
                    else:
                        st.warning(f"送信結果（成功:{success} / 失敗:{fail} / Ledger記録:{line_log_count}）")
                        with st.expander("失敗詳細", expanded=False):
                            st.write("\n".join(failed_list))

        st.divider()
        if not view_all.empty:
            st.markdown("#### 状態切替")

            status_options = []
            for _, r in view_all.iterrows():
                person_name = str(r["PersonName"]).strip()
                status_label = U.bool_to_status(r["IsActive"])
                status_options.append(f"{person_name} ｜ {status_label}")

            selected_label = st.selectbox("対象メンバー", status_options, key=f"status_target_{project}")
            selected_name = str(selected_label).split("｜")[0].strip()

            cur_row = view_all[view_all["PersonName"].astype(str).str.strip() == selected_name].iloc[0]
            current_status = U.bool_to_status(cur_row["IsActive"])
            next_status = AppConfig.STATUS["OFF"] if U.truthy(cur_row["IsActive"]) else AppConfig.STATUS["ON"]
            button_label = f"{current_status} → {next_status}"

            if st.button(button_label, use_container_width=True, key=f"toggle_status_{project}"):
                row_id = int(cur_row["_row_id"])
                ts = U.fmt_dt(U.now_jst())

                members_df.loc[row_id, "IsActive"] = not U.truthy(members_df.loc[row_id, "IsActive"])
                members_df.loc[row_id, "UpdatedAt_JST"] = ts

                msg = self.repo.validate_no_dup_lineid(members_df, project)
                if msg:
                    st.error(msg)
                    return

                self.repo.write_members(members_df)
                self.store.persist_and_refresh()
                st.success(f"{selected_name} を {next_status} に更新しました。")
                st.rerun()

        st.divider()
        if not view_all.empty:
            st.markdown("#### 一括編集（保存ボタンで確定）")
            edit_src = view_all.copy()
            edit_src["状態"] = edit_src["IsActive"].apply(U.bool_to_status)
            edit_show = edit_src[["PersonName", "Principal", "Rank", "状態", "Line_User_ID", "LINE_DisplayName"]].copy()
            row_ids = edit_src["_row_id"].tolist()

            edited = st.data_editor(
                edit_show,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config={
                    "Principal": st.column_config.NumberColumn("Principal", min_value=0.0, step=100.0),
                    "Rank": st.column_config.SelectboxColumn("Rank", options=[AppConfig.RANK["MASTER"], AppConfig.RANK["ELITE"]]),
                    "状態": st.column_config.SelectboxColumn("状態", options=[AppConfig.STATUS["ON"], AppConfig.STATUS["OFF"]]),
                },
                key=f"members_editor_{project}",
            )

            c1, c2 = st.columns([1, 1])
            save = c1.button("編集内容を保存", use_container_width=True, key=f"save_members_{project}")
            cancel = c2.button("編集を破棄（再読み込み）", use_container_width=True, key=f"cancel_members_{project}")

            if cancel:
                self.store.refresh()
                st.rerun()

            if save:
                ts = U.fmt_dt(U.now_jst())
                edited = edited.copy()
                edited["_row_id"] = row_ids

                for _, r in edited.iterrows():
                    row_id = int(r["_row_id"])
                    members_df.loc[row_id, "Principal"] = float(U.to_f(r["Principal"]))
                    members_df.loc[row_id, "Rank"] = U.normalize_rank(r["Rank"])
                    members_df.loc[row_id, "IsActive"] = U.status_to_bool(r["状態"])
                    members_df.loc[row_id, "Line_User_ID"] = str(r["Line_User_ID"]).strip()
                    members_df.loc[row_id, "LINE_DisplayName"] = str(r["LINE_DisplayName"]).strip()
                    members_df.loc[row_id, "UpdatedAt_JST"] = ts

                msg = self.repo.validate_no_dup_lineid(members_df, project)
                if msg:
                    st.error(msg)
                    return

                self.repo.write_members(members_df)
                self.store.persist_and_refresh()
                st.success("保存しました。")
                st.rerun()

        st.divider()
        st.markdown("#### 追加（同一プロジェクト内で Line_User_ID が一致したら追加しない）")

        add_mode = st.selectbox("追加先", ["個人(PERSONAL)", "プロジェクト"], key="member_add_mode")
        all_projects = self.repo.active_projects(settings_df)
        if add_mode == "個人(PERSONAL)":
            selected_project = AppConfig.PROJECT["PERSONAL"]
            st.info("登録先: PERSONAL")
        else:
            project_candidates = [p for p in all_projects if str(p).strip().upper() != AppConfig.PROJECT["PERSONAL"]]
            if not project_candidates:
                st.warning("PERSONAL以外のプロジェクトがありません。")
                return
            selected_project = st.selectbox("登録するプロジェクト", project_candidates, key="member_add_target_project")

        if line_users:
            labels = ["（選択しない）"] + [x[0] for x in line_users]
            picked = st.selectbox("登録済みLINEユーザーから選択", labels, index=0)
            if picked != "（選択しない）":
                idx = labels.index(picked) - 1
                _, uid, name = line_users[idx]
                st.session_state["prefill_line_uid"] = uid
                st.session_state["prefill_line_name"] = name

        pre_uid = st.session_state.get("prefill_line_uid", "")
        pre_name = st.session_state.get("prefill_line_name", "")
        with st.form("member_add", clear_on_submit=False):
            person = st.text_input("PersonName（個人名）")
            principal = st.number_input("Principal（残高）", min_value=0.0, value=0.0, step=100.0)
            line_uid = st.text_input("Line_User_ID（Uから始まる）", value=pre_uid)
            line_disp = st.text_input("LINE_DisplayName（任意）", value=pre_name)
            rank = st.selectbox("Rank", [AppConfig.RANK["MASTER"], AppConfig.RANK["ELITE"]], index=0)
            status = st.selectbox("ステータス", [AppConfig.STATUS["ON"], AppConfig.STATUS["OFF"]], index=0)
            submit = st.form_submit_button("保存（追加）")

        if submit:
            if not person or not line_uid:
                st.error("PersonName と Line_User_ID は必須です。")
                return

            exists = members_df[
                (members_df["Project_Name"] == str(selected_project))
                & (members_df["Line_User_ID"].astype(str).str.strip() == str(line_uid).strip())
            ]
            if not exists.empty:
                st.warning("このプロジェクト内に同じ Line_User_ID が既に存在します。")
                return

            ts = U.fmt_dt(U.now_jst())
            new_row = {
                "Project_Name": str(selected_project).strip(),
                "PersonName": str(person).strip(),
                "Principal": float(principal),
                "Line_User_ID": str(line_uid).strip(),
                "LINE_DisplayName": str(line_disp).strip(),
                "Rank": U.normalize_rank(rank),
                "IsActive": U.status_to_bool(status),
                "CreatedAt_JST": ts,
                "UpdatedAt_JST": ts,
            }
            members_df = pd.concat([members_df, pd.DataFrame([new_row])], ignore_index=True)

            msg = self.repo.validate_no_dup_lineid(members_df, selected_project)
            if msg:
                st.error(msg)
                return

            self.repo.write_members(members_df)
            self.store.persist_and_refresh()
            st.success(f"追加しました。登録先: {selected_project}")
            st.rerun()

    def render_help(self, gs: GSheetService, settings_df: pd.DataFrame) -> None:
        st.subheader("❓ ヘルプ / 使い方")
        st.caption(f"{AppConfig.RANK_LABEL} / 管理者: {AdminAuth.current_label()}")

        st.markdown(
            """
このアプリは、APR運用の記録、入出金、メンバー管理、LINE通知をまとめて扱う管理システムです。
左メニューの **📊 ダッシュボード / 📈 APR / 💸 入金/出金 / ⚙️ 管理 / ❓ ヘルプ** で画面を切り替えます。
"""
        )

        with st.expander("1. 現在の接続情報", expanded=False):
            st.code(
                f"""参照シート
Settings           = {gs.names.SETTINGS}
Members            = {gs.names.MEMBERS}
Ledger             = {gs.names.LEDGER}
LineUsers          = {gs.names.LINEUSERS}
APR_Summary        = {gs.names.APR_SUMMARY}
SmartVault_History = {gs.names.SMARTVAULT_HISTORY}
USDC_History       = {gs.names.USDC_HISTORY}

Spreadsheet ID
{gs.spreadsheet_id}

Spreadsheet URL
{gs.spreadsheet_url()}
"""
            )

        with st.expander("2. シート構成", expanded=False):
            st.markdown("### Settings")
            st.code("\t".join(AppConfig.HEADERS["SETTINGS"]))
            st.markdown("### Members")
            st.code("\t".join(AppConfig.HEADERS["MEMBERS"]))
            st.markdown("### Ledger")
            st.code("\t".join(AppConfig.HEADERS["LEDGER"]))
            st.markdown("### LineUsers")
            st.code("\t".join(AppConfig.HEADERS["LINEUSERS"]))
            st.markdown("### APR Summary")
            st.code("\t".join(AppConfig.HEADERS["APR_SUMMARY"]))
            st.markdown("### SmartVault_History")
            st.code("\t".join(AppConfig.HEADERS["SMARTVAULT_HISTORY"]))
            st.markdown("### USDC_History")
            st.code("\t".join(AppConfig.HEADERS["USDC_HISTORY"]))

        with st.expander("3. Compound_Timing の意味", expanded=False):
            st.markdown(
                """
- `daily`
  APR確定時に元本へ即時加算します。次回以降は増えた元本で計算します。

- `monthly`
  APR確定時は Ledger に記録のみ行います。元本への反映は APR画面の「未反映APRを元本へ反映」でまとめて行います。

- `none`
  単利です。APRは Ledger に記録しますが、元本には加算しません。
"""
            )

        with st.expander("4. APR計算ロジック", expanded=False):
            st.markdown(
                """
### 入力項目
APR画面では以下を管理します。

- 流動性
- 昨日の収益
- APR

いずれも手動入力できます。画像を入れた場合は OCRで別取得 もできます。

### OCR
Smart Vaultモバイル画面では固定ボックスで
- 総流動性
- 昨日の収益
- APR
を別々にOCRしています。

### 📱 / 🖥️ デバイス別 APR 採用ルール

画像をOCRしたときに自動でデバイスを判定し、採用するAPR値が変わります。

| デバイス | 採用APR | 理由 |
|---------|--------|------|
| 📱 **モバイル** | OCR取得APR をそのまま使用 | SmartVault の正確な値 |
| 🖥️ **PC** | OCR取得APR × **66%** | PC表示は実際より高めに出るため補正 |

複数回OCRした場合（PC＋モバイル）は **モバイル値を優先** します。

---

### 📊 SmartVault履歴
APR確定時に `SmartVault_History` シートへ
- 最終採用値
- OCR取得値
- Source_Mode（manual / ocr / ocr+manual）
- Device_Type（pc / mobile）
を保存します。

---

### 💰 個人への分配計算

#### PERSONAL
個人ごとの元本で計算します。

`DailyAPR = Principal × (採用APR% / 100) × Rank係数 ÷ 365`

- Master = 0.67
- Elite = 0.60

#### GROUP（PERSONAL以外）
グループ総額を基準に計算し、人数で均等割します。

`グループ総配当 = グループ総元本 × (採用APR% / 100) × Net_Factor ÷ 365`

`1人あたり配当 = グループ総配当 ÷ 人数`

> **採用APR** = PC画像なら元のAPR × 0.66、モバイル画像なら元のAPRのまま

---

### 🔄 重複防止
同日・同一プロジェクト・同一人物の APR は Ledger を見て1回だけ記録します。
本日のAPRをやり直したい場合は、APR画面の「本日のAPR記録をリセット」を使います。
"""
            )

        with st.expander("5. Make連携", expanded=False):
            st.markdown(
                """
### 目的
LINEユーザー情報を `LineUsers` シートへ自動登録し、管理画面の追加候補として使います。

### 推奨フロー
`LINE Watch Events → HTTP(プロフィール取得) → Google Sheets Search Rows → Filter(0件のみ) → Google Sheets Add a Row`
"""
            )
            st.code("\t".join(AppConfig.HEADERS["LINEUSERS"]))

        with st.expander("6. Settings自動修復", expanded=False):
            st.markdown(
                """
Settings シートの不足列補完、PERSONAL行の不足補完、OCR初期座標の補完を行います。
シート構造が崩れたときはこちらを実行してください。
"""
            )
            if st.button("Settingsを自動修復", key="help_fix_settings", use_container_width=True):
                try:
                    self.repo.repair_settings(self.repo.load_settings())
                    self.store.persist_and_refresh()
                    st.success(f"{self.repo.gs.names.SETTINGS} を修復しました。")
                    st.rerun()
                except Exception as e:
                    st.error(f"Settings修復でエラー: {e}")

        with st.expander("7. OCR設定（座標設定 + 赤枠プレビュー）", expanded=False):
            projects = self.repo.active_projects(settings_df)
            if not projects:
                st.warning("有効なプロジェクトがありません。")
                return

            ocr_project = st.selectbox("OCR設定対象プロジェクト", projects, key="help_ocr_project")
            row_setting = settings_df[settings_df["Project_Name"] == ocr_project].iloc[0]

            st.markdown("#### 現在値")
            _all_defaults = {
                **AppConfig.OCR_DEFAULTS_PC,
                **AppConfig.OCR_DEFAULTS_MOBILE,
                **AppConfig.SV_BOX_DEFAULTS,
                **AppConfig.PC_BOX_DEFAULTS,
            }
            _cv_rows = []
            for _zone, _keys in [
                ("🖥️ PC APR",          ["Crop_Left_Ratio_PC", "Crop_Top_Ratio_PC", "Crop_Right_Ratio_PC", "Crop_Bottom_Ratio_PC"]),
                ("🖥️ PC 流動性",       ["PC_Liq_Left", "PC_Liq_Top", "PC_Liq_Right", "PC_Liq_Bottom"]),
                ("🖥️ PC 昨日の収益",   ["PC_Profit_Left", "PC_Profit_Top", "PC_Profit_Right", "PC_Profit_Bottom"]),
                ("📱 Mobile APR補助",  ["Crop_Left_Ratio_Mobile", "Crop_Top_Ratio_Mobile", "Crop_Right_Ratio_Mobile", "Crop_Bottom_Ratio_Mobile"]),
                ("📱 SV 流動性",       ["SV_Liq_Left", "SV_Liq_Top", "SV_Liq_Right", "SV_Liq_Bottom"]),
                ("📱 SV 昨日の収益",   ["SV_Profit_Left", "SV_Profit_Top", "SV_Profit_Right", "SV_Profit_Bottom"]),
                ("📱 SV APR",         ["SV_APR_Left", "SV_APR_Top", "SV_APR_Right", "SV_APR_Bottom"]),
            ]:
                vals = [float(row_setting.get(k, _all_defaults.get(k, 0))) for k in _keys]
                _cv_rows.append({"ゾーン": _zone, "Left": vals[0], "Top": vals[1], "Right": vals[2], "Bottom": vals[3]})
            st.dataframe(pd.DataFrame(_cv_rows), use_container_width=True, hide_index=True)

            st.markdown("#### 座標入力")
            sv_d = AppConfig.SV_BOX_DEFAULTS
            pc_d = AppConfig.PC_BOX_DEFAULTS

            st.markdown("##### 🖥️ PC — APRゾーン（従来）")
            c1, c2, c3, c4 = st.columns(4)
            pc_left   = c1.number_input("Left",   0.0, 1.0, float(row_setting.get("Crop_Left_Ratio_PC",   AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"])),   0.01, key=f"help_pc_left_{ocr_project}")
            pc_top    = c2.number_input("Top",    0.0, 1.0, float(row_setting.get("Crop_Top_Ratio_PC",    AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"])),    0.01, key=f"help_pc_top_{ocr_project}")
            pc_right  = c3.number_input("Right",  0.0, 1.0, float(row_setting.get("Crop_Right_Ratio_PC",  AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"])),  0.01, key=f"help_pc_right_{ocr_project}")
            pc_bottom = c4.number_input("Bottom", 0.0, 1.0, float(row_setting.get("Crop_Bottom_Ratio_PC", AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"])), 0.01, key=f"help_pc_bottom_{ocr_project}")

            st.markdown("##### 🖥️ PC — 流動性ゾーン")
            ca, cb, cc, cd = st.columns(4)
            pc_liq_left   = ca.number_input("Left",   0.0, 1.0, float(row_setting.get("PC_Liq_Left",   pc_d["PC_Liq_Left"])),   0.01, key=f"help_pc_liq_left_{ocr_project}")
            pc_liq_top    = cb.number_input("Top",    0.0, 1.0, float(row_setting.get("PC_Liq_Top",    pc_d["PC_Liq_Top"])),    0.01, key=f"help_pc_liq_top_{ocr_project}")
            pc_liq_right  = cc.number_input("Right",  0.0, 1.0, float(row_setting.get("PC_Liq_Right",  pc_d["PC_Liq_Right"])),  0.01, key=f"help_pc_liq_right_{ocr_project}")
            pc_liq_bottom = cd.number_input("Bottom", 0.0, 1.0, float(row_setting.get("PC_Liq_Bottom", pc_d["PC_Liq_Bottom"])), 0.01, key=f"help_pc_liq_bottom_{ocr_project}")

            st.markdown("##### 🖥️ PC — 昨日の収益ゾーン")
            ce, cf, cg, ch = st.columns(4)
            pc_profit_left   = ce.number_input("Left",   0.0, 1.0, float(row_setting.get("PC_Profit_Left",   pc_d["PC_Profit_Left"])),   0.01, key=f"help_pc_profit_left_{ocr_project}")
            pc_profit_top    = cf.number_input("Top",    0.0, 1.0, float(row_setting.get("PC_Profit_Top",    pc_d["PC_Profit_Top"])),    0.01, key=f"help_pc_profit_top_{ocr_project}")
            pc_profit_right  = cg.number_input("Right",  0.0, 1.0, float(row_setting.get("PC_Profit_Right",  pc_d["PC_Profit_Right"])),  0.01, key=f"help_pc_profit_right_{ocr_project}")
            pc_profit_bottom = ch.number_input("Bottom", 0.0, 1.0, float(row_setting.get("PC_Profit_Bottom", pc_d["PC_Profit_Bottom"])), 0.01, key=f"help_pc_profit_bottom_{ocr_project}")

            st.markdown("##### 📱 Mobile — APR補助ゾーン（従来）")
            c5, c6, c7, c8 = st.columns(4)
            mobile_left   = c5.number_input("Left",   0.0, 1.0, float(row_setting.get("Crop_Left_Ratio_Mobile",   AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"])),   0.01, key=f"help_mobile_left_{ocr_project}")
            mobile_top    = c6.number_input("Top",    0.0, 1.0, float(row_setting.get("Crop_Top_Ratio_Mobile",    AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"])),    0.01, key=f"help_mobile_top_{ocr_project}")
            mobile_right  = c7.number_input("Right",  0.0, 1.0, float(row_setting.get("Crop_Right_Ratio_Mobile",  AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"])),  0.01, key=f"help_mobile_right_{ocr_project}")
            mobile_bottom = c8.number_input("Bottom", 0.0, 1.0, float(row_setting.get("Crop_Bottom_Ratio_Mobile", AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"])), 0.01, key=f"help_mobile_bottom_{ocr_project}")

            st.markdown("##### 📱 SmartVault Mobile — 流動性ゾーン")
            sa, sb, sc, sd = st.columns(4)
            sv_liq_left   = sa.number_input("Left",   0.0, 1.0, float(row_setting.get("SV_Liq_Left",   sv_d["SV_Liq_Left"])),   0.01, key=f"help_sv_liq_left_{ocr_project}")
            sv_liq_top    = sb.number_input("Top",    0.0, 1.0, float(row_setting.get("SV_Liq_Top",    sv_d["SV_Liq_Top"])),    0.01, key=f"help_sv_liq_top_{ocr_project}")
            sv_liq_right  = sc.number_input("Right",  0.0, 1.0, float(row_setting.get("SV_Liq_Right",  sv_d["SV_Liq_Right"])),  0.01, key=f"help_sv_liq_right_{ocr_project}")
            sv_liq_bottom = sd.number_input("Bottom", 0.0, 1.0, float(row_setting.get("SV_Liq_Bottom", sv_d["SV_Liq_Bottom"])), 0.01, key=f"help_sv_liq_bottom_{ocr_project}")

            st.markdown("##### 📱 SmartVault Mobile — 昨日の収益ゾーン")
            se, sf, sg, sh = st.columns(4)
            sv_profit_left   = se.number_input("Left",   0.0, 1.0, float(row_setting.get("SV_Profit_Left",   sv_d["SV_Profit_Left"])),   0.01, key=f"help_sv_profit_left_{ocr_project}")
            sv_profit_top    = sf.number_input("Top",    0.0, 1.0, float(row_setting.get("SV_Profit_Top",    sv_d["SV_Profit_Top"])),    0.01, key=f"help_sv_profit_top_{ocr_project}")
            sv_profit_right  = sg.number_input("Right",  0.0, 1.0, float(row_setting.get("SV_Profit_Right",  sv_d["SV_Profit_Right"])),  0.01, key=f"help_sv_profit_right_{ocr_project}")
            sv_profit_bottom = sh.number_input("Bottom", 0.0, 1.0, float(row_setting.get("SV_Profit_Bottom", sv_d["SV_Profit_Bottom"])), 0.01, key=f"help_sv_profit_bottom_{ocr_project}")

            st.markdown("##### 📱 SmartVault Mobile — APRゾーン")
            si, sj, sk, sl_ = st.columns(4)
            sv_apr_left   = si.number_input("Left",   0.0, 1.0, float(row_setting.get("SV_APR_Left",   sv_d["SV_APR_Left"])),   0.01, key=f"help_sv_apr_left_{ocr_project}")
            sv_apr_top    = sj.number_input("Top",    0.0, 1.0, float(row_setting.get("SV_APR_Top",    sv_d["SV_APR_Top"])),    0.01, key=f"help_sv_apr_top_{ocr_project}")
            sv_apr_right  = sk.number_input("Right",  0.0, 1.0, float(row_setting.get("SV_APR_Right",  sv_d["SV_APR_Right"])),  0.01, key=f"help_sv_apr_right_{ocr_project}")
            sv_apr_bottom = sl_.number_input("Bottom", 0.0, 1.0, float(row_setting.get("SV_APR_Bottom", sv_d["SV_APR_Bottom"])), 0.01, key=f"help_sv_apr_bottom_{ocr_project}")

            st.markdown("#### OCR確認用画像アップロード")
            preview = st.file_uploader(
                "画像をアップロードすると赤枠プレビューします",
                type=["png", "jpg", "jpeg"],
                key="help_ocr_preview",
            )

            if preview is not None:
                try:
                    file_bytes = preview.getvalue()
                    is_mobile_preview = U.is_mobile_tall_image(file_bytes)
                    st.info(f"画像タイプ判定: {'📱 モバイル' if is_mobile_preview else '🖥️ PC'}")

                    st.markdown("##### 元画像")
                    st.image(file_bytes, caption="元画像", use_container_width=True)

                    if is_mobile_preview:
                        sv_preview_boxes = {
                            "TOTAL_LIQUIDITY": {"left": float(sv_liq_left), "top": float(sv_liq_top), "right": float(sv_liq_right), "bottom": float(sv_liq_bottom)},
                            "YESTERDAY_PROFIT": {"left": float(sv_profit_left), "top": float(sv_profit_top), "right": float(sv_profit_right), "bottom": float(sv_profit_bottom)},
                            "APR": {"left": float(sv_apr_left), "top": float(sv_apr_top), "right": float(sv_apr_right), "bottom": float(sv_apr_bottom)},
                        }
                        st.markdown("##### 📱 SmartVault 3ゾーン赤枠プレビュー")
                        st.caption(
                            "⚠️ この赤枠は **SmartVaultサマリー画面専用** です。"
                            "USDC取引履歴スクリーンショットには適用されません（取引履歴は全画面OCRで自動読み取り）。"
                        )
                        sv_boxed = U.draw_ocr_boxes(file_bytes, sv_preview_boxes)
                        st.image(sv_boxed, caption="SmartVault 3ゾーン設定", use_container_width=True)

                        mobile_aux_box = {"APR補助": {"left": float(mobile_left), "top": float(mobile_top), "right": float(mobile_right), "bottom": float(mobile_bottom)}}
                        st.markdown("##### 📱 Mobile APR補助ゾーン赤枠プレビュー")
                        st.image(U.draw_ocr_boxes(file_bytes, mobile_aux_box), caption="Mobile APR補助", use_container_width=True)
                    else:
                        pc_preview_boxes = {
                            "TOTAL_LIQUIDITY": {"left": float(pc_liq_left), "top": float(pc_liq_top), "right": float(pc_liq_right), "bottom": float(pc_liq_bottom)},
                            "YESTERDAY_PROFIT": {"left": float(pc_profit_left), "top": float(pc_profit_top), "right": float(pc_profit_right), "bottom": float(pc_profit_bottom)},
                            "APR": {"left": float(pc_left), "top": float(pc_top), "right": float(pc_right), "bottom": float(pc_bottom)},
                        }
                        st.markdown("##### 🖥️ PC 3ゾーン赤枠プレビュー")
                        pc_boxed = U.draw_ocr_boxes(file_bytes, pc_preview_boxes)
                        st.image(pc_boxed, caption="PC 3ゾーン設定", use_container_width=True)

                except Exception as e:
                    st.error(f"赤枠プレビュー表示でエラー: {e}")

            if st.button("OCR座標を保存", key=f"help_save_ocr_{ocr_project}", use_container_width=True):
                try:
                    idx = settings_df[settings_df["Project_Name"] == ocr_project].index[0]
                    # PC APR zone
                    settings_df.loc[idx, "Crop_Left_Ratio_PC"]   = U.to_ratio(pc_left,   AppConfig.OCR_DEFAULTS_PC["Crop_Left_Ratio_PC"])
                    settings_df.loc[idx, "Crop_Top_Ratio_PC"]    = U.to_ratio(pc_top,    AppConfig.OCR_DEFAULTS_PC["Crop_Top_Ratio_PC"])
                    settings_df.loc[idx, "Crop_Right_Ratio_PC"]  = U.to_ratio(pc_right,  AppConfig.OCR_DEFAULTS_PC["Crop_Right_Ratio_PC"])
                    settings_df.loc[idx, "Crop_Bottom_Ratio_PC"] = U.to_ratio(pc_bottom, AppConfig.OCR_DEFAULTS_PC["Crop_Bottom_Ratio_PC"])
                    # PC liquidity zone
                    settings_df.loc[idx, "PC_Liq_Left"]   = U.to_ratio(pc_liq_left,   pc_d["PC_Liq_Left"])
                    settings_df.loc[idx, "PC_Liq_Top"]    = U.to_ratio(pc_liq_top,    pc_d["PC_Liq_Top"])
                    settings_df.loc[idx, "PC_Liq_Right"]  = U.to_ratio(pc_liq_right,  pc_d["PC_Liq_Right"])
                    settings_df.loc[idx, "PC_Liq_Bottom"] = U.to_ratio(pc_liq_bottom, pc_d["PC_Liq_Bottom"])
                    # PC profit zone
                    settings_df.loc[idx, "PC_Profit_Left"]   = U.to_ratio(pc_profit_left,   pc_d["PC_Profit_Left"])
                    settings_df.loc[idx, "PC_Profit_Top"]    = U.to_ratio(pc_profit_top,    pc_d["PC_Profit_Top"])
                    settings_df.loc[idx, "PC_Profit_Right"]  = U.to_ratio(pc_profit_right,  pc_d["PC_Profit_Right"])
                    settings_df.loc[idx, "PC_Profit_Bottom"] = U.to_ratio(pc_profit_bottom, pc_d["PC_Profit_Bottom"])
                    # Mobile APR aux zone
                    settings_df.loc[idx, "Crop_Left_Ratio_Mobile"]   = U.to_ratio(mobile_left,   AppConfig.OCR_DEFAULTS_MOBILE["Crop_Left_Ratio_Mobile"])
                    settings_df.loc[idx, "Crop_Top_Ratio_Mobile"]    = U.to_ratio(mobile_top,    AppConfig.OCR_DEFAULTS_MOBILE["Crop_Top_Ratio_Mobile"])
                    settings_df.loc[idx, "Crop_Right_Ratio_Mobile"]  = U.to_ratio(mobile_right,  AppConfig.OCR_DEFAULTS_MOBILE["Crop_Right_Ratio_Mobile"])
                    settings_df.loc[idx, "Crop_Bottom_Ratio_Mobile"] = U.to_ratio(mobile_bottom, AppConfig.OCR_DEFAULTS_MOBILE["Crop_Bottom_Ratio_Mobile"])
                    # SV Mobile liquidity zone
                    settings_df.loc[idx, "SV_Liq_Left"]   = U.to_ratio(sv_liq_left,   sv_d["SV_Liq_Left"])
                    settings_df.loc[idx, "SV_Liq_Top"]    = U.to_ratio(sv_liq_top,    sv_d["SV_Liq_Top"])
                    settings_df.loc[idx, "SV_Liq_Right"]  = U.to_ratio(sv_liq_right,  sv_d["SV_Liq_Right"])
                    settings_df.loc[idx, "SV_Liq_Bottom"] = U.to_ratio(sv_liq_bottom, sv_d["SV_Liq_Bottom"])
                    # SV Mobile profit zone
                    settings_df.loc[idx, "SV_Profit_Left"]   = U.to_ratio(sv_profit_left,   sv_d["SV_Profit_Left"])
                    settings_df.loc[idx, "SV_Profit_Top"]    = U.to_ratio(sv_profit_top,    sv_d["SV_Profit_Top"])
                    settings_df.loc[idx, "SV_Profit_Right"]  = U.to_ratio(sv_profit_right,  sv_d["SV_Profit_Right"])
                    settings_df.loc[idx, "SV_Profit_Bottom"] = U.to_ratio(sv_profit_bottom, sv_d["SV_Profit_Bottom"])
                    # SV Mobile APR zone
                    settings_df.loc[idx, "SV_APR_Left"]   = U.to_ratio(sv_apr_left,   sv_d["SV_APR_Left"])
                    settings_df.loc[idx, "SV_APR_Top"]    = U.to_ratio(sv_apr_top,    sv_d["SV_APR_Top"])
                    settings_df.loc[idx, "SV_APR_Right"]  = U.to_ratio(sv_apr_right,  sv_d["SV_APR_Right"])
                    settings_df.loc[idx, "SV_APR_Bottom"] = U.to_ratio(sv_apr_bottom, sv_d["SV_APR_Bottom"])

                    settings_df.loc[idx, "UpdatedAt_JST"] = U.fmt_dt(U.now_jst())

                    self.repo.write_settings(settings_df)
                    self.store.persist_and_refresh()
                    st.success("OCR設定を保存しました。")
                    st.rerun()
                except Exception as e:
                    st.error(f"OCR設定保存でエラー: {e}")


# =========================================================
# APP CONTROLLER
# =========================================================
class AppController:
    def __init__(self):
        self.gs: Optional[GSheetService] = None
        self.repo: Optional[Repository] = None
        self.engine: Optional[FinanceEngine] = None
        self.store: Optional[DataStore] = None
        self.ui: Optional[AppUI] = None

    def setup_page(self) -> None:
        st.set_page_config(page_title=AppConfig.APP_TITLE, layout=AppConfig.PAGE_LAYOUT, page_icon=AppConfig.APP_ICON)
        st.title(f"{AppConfig.APP_ICON} {AppConfig.APP_TITLE}")

    def setup_auth(self) -> None:
        AdminAuth.require_login()
        st.markdown(
            """
            <style>
              section[data-testid="stSidebar"] div[role="radiogroup"] > label { margin: 10px 0 !important; padding: 6px 8px !important; }
              section[data-testid="stSidebar"] div[role="radiogroup"] > label p { font-size: 16px !important; }
            </style>
            """,
            unsafe_allow_html=True,
        )
        with st.sidebar:
            st.caption(f"👤 {AdminAuth.current_label()}")
            if st.button("🔓 ログアウト", use_container_width=True):
                st.session_state["admin_ok"] = False
                st.session_state["admin_name"] = ""
                st.session_state["admin_namespace"] = ""
                for key in AppConfig.SESSION_KEYS.values():
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()

    def setup_state(self) -> None:
        if "page" not in st.session_state:
            st.session_state["page"] = AppConfig.PAGE["DASHBOARD"]
        if "hide_line_history" not in st.session_state:
            st.session_state["hide_line_history"] = False

    def setup_services(self) -> None:
        con = st.secrets.get("connections", {}).get("gsheets", {})
        sid = U.extract_sheet_id(str(con.get("spreadsheet", "")).strip())
        if not sid:
            st.error("Secrets の [connections.gsheets].spreadsheet が未設定です。")
            st.stop()

        try:
            self.gs = GSheetService(spreadsheet_id=sid, namespace=AdminAuth.current_namespace())
        except Exception as e:
            msg = str(e)
            if "Quota exceeded" in msg or "429" in msg:
                st.error("Google Sheets API の読み取り上限に達しています。1〜2分待ってから再読み込みしてください。")
            else:
                st.error(f"Spreadsheet を開けません。: {e}")
            st.stop()

        self.repo = Repository(self.gs)
        self.engine = FinanceEngine()
        self.store = DataStore(self.repo, self.engine)
        self.ui = AppUI(self.repo, self.engine, self.store)

    def run(self) -> None:
        self.setup_page()
        self.setup_auth()
        self.setup_state()
        self.setup_services()

        data = self.store.load(force=False)
        menu = [
            AppConfig.PAGE["DASHBOARD"],
            AppConfig.PAGE["APR"],
            AppConfig.PAGE["CASH"],
            AppConfig.PAGE["ADMIN"],
            AppConfig.PAGE["HELP"],
        ]
        page = st.sidebar.radio("メニュー", options=menu, index=menu.index(st.session_state["page"]) if st.session_state["page"] in menu else 0)
        st.session_state["page"] = page

        if page == AppConfig.PAGE["DASHBOARD"]:
            self.repo.write_apr_summary(data["apr_summary_df"])
            self.ui.render_dashboard(data["members_df"], data["ledger_df"], data["apr_summary_df"])
        elif page == AppConfig.PAGE["APR"]:
            self.ui.render_apr(data["settings_df"], data["members_df"])
        elif page == AppConfig.PAGE["CASH"]:
            self.ui.render_cash(data["settings_df"], data["members_df"])
        elif page == AppConfig.PAGE["ADMIN"]:
            self.ui.render_admin(data["settings_df"], data["members_df"], data["line_users_df"])
        else:
            self.ui.render_help(self.gs, data["settings_df"])


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    AppController().run()


if __name__ == "__main__":
    main()
