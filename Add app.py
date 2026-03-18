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
        try:
            img = Image.open(BytesIO(file_bytes))
            w, h = img.size
            return h / max(w, 1) > 1.45
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
    ) -> str:
        try:
            api_key = st.secrets["ocrspace"]["api_key"]
        except Exception:
            return ""

        texts: List[str] = []

        try:
            cropped_bytes = U.crop_image_by_ratio(
                file_bytes=file_bytes,
                left_ratio=crop_left_ratio,
                top_ratio=crop_top_ratio,
                right_ratio=crop_right_ratio,
                bottom_ratio=crop_bottom_ratio,
            )

            processed_list = U.preprocess_ocr_image(cropped_bytes)
            targets = [("cropped.png", cropped_bytes)] + [(f"processed_{i}.png", b) for i, b in enumerate(processed_list, start=1)]

            for target_name, target_bytes in targets:
                for engine in (2, 1):
                    try:
                        res = requests.post(
                            "https://api.ocr.space/parse/image",
                            files={"filename": (target_name, target_bytes)},
                            data={
                                "apikey": api_key,
                                "language": "eng",
                                "isOverlayRequired": False,
                                "OCREngine": engine,
                                "scale": True,
                                "detectOrientation": True,
                                "isTable": False,
                            },
                            timeout=60,
                        )
                        data = res.json()
                        for p in data.get("ParsedResults", []):
                            txt = str(p.get("ParsedText", "")).strip()
                            if txt:
                                texts.append(txt)
                    except Exception:
                        continue

            uniq, seen = [], set()
            for t in texts:
                key = t.strip()
                if key and key not in seen:
                    seen.add(key)
                    uniq.append(key)

            return "\n\n".join(uniq)

        except Exception:
            return ""


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
        self.gc = gspread.authorize(creds)
        self.book = self.gc.open_by_key(self.spreadsheet_id)

        ensure_key = (
            f"_sheet_ensured_{self.names.SETTINGS}_{self.names.MEMBERS}_{self.names.LEDGER}_"
            f"{self.names.LINEUSERS}_{self.names.APR_SUMMARY}_{self.names.SMARTVAULT_HISTORY}"
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
            ws.update("1:1", [colset + missing])

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
        # =========================================================
# REPOSITORY
# =========================================================
class Repository:
    def __init__(self, gs: GSheetService):
        self.gs = gs

    def _ensure_setting_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            if k not in out.columns:
                out[k] = v
            else:
                out[k] = out[k].replace("", v)
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
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

        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            df[k] = df[k].apply(lambda x, default=v: U.to_ratio(x, default))
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
            df[k] = df[k].apply(lambda x, default=v: U.to_ratio(x, default))

        personal_df = df[df["Project_Name"].str.upper() == AppConfig.PROJECT["PERSONAL"]].tail(1).copy()
        other_df = df[df["Project_Name"].str.upper() != AppConfig.PROJECT["PERSONAL"]].drop_duplicates(
            subset=["Project_Name"],
            keep="last",
        )
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

        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            out[k] = out[k].apply(lambda x, default=v: f"{U.to_ratio(x, default):.3f}")
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
            out[k] = out[k].apply(lambda x, default=v: f"{U.to_ratio(x, default):.3f}")

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
        other_df = repaired[repaired["Project_Name"].str.upper() != AppConfig.PROJECT["PERSONAL"]].drop_duplicates(
            subset=["Project_Name"],
            keep="last",
        )
        repaired = pd.concat([personal_df, other_df], ignore_index=True)

        repaired["Net_Factor"] = U.to_num_series(repaired["Net_Factor"], AppConfig.FACTOR["MASTER"])
        repaired.loc[repaired["Net_Factor"] <= 0, "Net_Factor"] = AppConfig.FACTOR["MASTER"]
        repaired["IsCompound"] = repaired["IsCompound"].apply(U.truthy)
        repaired["Compound_Timing"] = repaired["Compound_Timing"].apply(U.normalize_compound)
        repaired["Active"] = repaired["Active"].apply(lambda x: U.truthy(x) if str(x).strip() else True)
        repaired["UpdatedAt_JST"] = repaired["UpdatedAt_JST"].astype(str) if "UpdatedAt_JST" in repaired.columns else ""

        for k, v in AppConfig.OCR_DEFAULTS_PC.items():
            repaired[k] = repaired[k].apply(lambda x, default=v: U.to_ratio(x, default))
        for k, v in AppConfig.OCR_DEFAULTS_MOBILE.items():
            repaired[k] = repaired[k].apply(lambda x, default=v: U.to_ratio(x, default))

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
            ],
        )

    def active_projects(self, settings_df: pd.DataFrame) -> List[str]:
        if settings_df.empty:
            return []
        return settings_df.loc[settings_df["Active"] == True, "Project_Name"].dropna().astype(str).unique().tolist()

    def project_members_active(self, members_df: pd.DataFrame, project: str) -> pd.DataFrame:
        if members_df.empty:
            return members_df.copy()
        return members_df[
            (members_df["Project_Name"] == str(project))
            & (members_df["IsActive"] == True)
        ].copy().reset_index(drop=True)

    def validate_no_dup_lineid(self, members_df: pd.DataFrame, project: str) -> Optional[str]:
        if members_df.empty:
            return None
        df = members_df[members_df["Project_Name"] == str(project)].copy()
        df["Line_User_ID"] = df["Line_User_ID"].astype(str).str.strip()
        df = df[df["Line_User_ID"] != ""]
        dup = df[df.duplicated(subset=["Line_User_ID"], keep=False)]
        return None if dup.empty else f"同一プロジェクト内で Line_User_ID が重複しています: {dup['Line_User_ID'].unique().tolist()}"

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
        return set(
            zip(
                df["Project_Name"].astype(str).str.strip(),
                df["PersonName"].astype(str).str.strip(),
            )
        )

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
            kept_rows.append(row[:len(headers)])

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

        summary = apr_df.groupby("PersonName", as_index=False).agg(
            Total_APR=("Amount", "sum"),
            APR_Count=("Amount", "count")
        )
        disp_map = apr_df.sort_values("Datetime_JST", ascending=False).drop_duplicates(subset=["PersonName"])[
            ["PersonName", "LINE_DisplayName"]
        ].copy()
        summary = summary.merge(disp_map, on="PersonName", how="left")
        summary["Date_JST"] = U.fmt_date(U.now_jst())
        summary["Asset_Ratio"] = summary["Total_APR"].map(
            lambda x: f"{(float(x) / total_assets) * 100:.2f}%" if total_assets > 0 else "0.00%"
        )
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
        mask = (
            members_df["Project_Name"].astype(str).str.strip() == str(project).strip()
        ) & (
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

    def dashboard(self, data: dict):
        st.header("📊 ダッシュボード")

        members = data["members_df"]
        ledger = data["ledger_df"]

        if members.empty:
            st.info("メンバーが登録されていません")
            return

        total = members["Principal"].sum()

        col1, col2 = st.columns(2)

        with col1:
            st.metric("総資産", f"${total:,.2f}")

        today = U.fmt_date(U.now_jst())
        today_profit = ledger[
            (ledger["Type"] == AppConfig.TYPE["APR"]) &
            (ledger["Datetime_JST"].str.startswith(today))
        ]["Amount"].sum()

        with col2:
            st.metric("本日収益", f"${today_profit:,.2f}")

        st.subheader("メンバー")

        st.dataframe(
            members[["PersonName", "Principal", "Rank"]],
            use_container_width=True
        )


    def apr_screen(self, data: dict):
        st.header("📈 APR登録")

        apr = st.number_input(
            "APR (%)",
            min_value=0.0,
            max_value=1000.0,
            value=50.0,
            step=0.01
        )

        run = st.button("APR実行")

        return run, apr


    def cash_screen(self, repo: Repository, members: pd.DataFrame):
        st.header("💸 入出金")

        person = st.selectbox("メンバー", members["PersonName"].tolist())

        amount = st.number_input("金額", value=0.0)

        col1, col2 = st.columns(2)

        if col1.button("入金"):
            repo.append_ledger(
                U.fmt_dt(U.now_jst()),
                "PERSONAL",
                person,
                "Deposit",
                amount,
                ""
            )

        if col2.button("出金"):
            repo.append_ledger(
                U.fmt_dt(U.now_jst()),
                "PERSONAL",
                person,
                "Withdraw",
                amount,
                ""
            )


    def help_page(self):
        st.header("❓ヘルプ")

        st.markdown(
        """
### APR運用管理システム

機能

・APR登録  
・日次利益分配  
・LINE自動通知  
・OCR APR取得  

LINE通知は  

OCR → APR確定 → 自動送信  

で動作します。
"""
        )


# =========================================================
# CONTROLLER
# =========================================================
class AppController:

    def __init__(self):
sheet_id = U.extract_sheet_id(st.secrets["connections"]["gsheets"]["spreadsheet"])

        namespace = AdminAuth.current_namespace()

        gs = GSheetService(sheet_id, namespace)

        self.repo = Repository(gs)

        self.engine = FinanceEngine()

        self.store = DataStore(self.repo, self.engine)

        self.ui = AppUI()


    def send_line(self, project, result_df):

        token = ExternalService.get_line_token(
            AdminAuth.current_namespace()
        )

        for _, row in result_df.iterrows():

            uid = row["Line_User_ID"]

            if not uid:
                continue

            text = f"""
【SmartVault報告】

{row["PersonName"]}様

本日の収益

${row["DailyAPR"]:.2f}

"""

            ExternalService.send_line_push(
                token,
                uid,
                text
            )


    def execute_apr(self, apr):

        data = self.store.load()

        members = data["members_df"]

        settings = data["settings_df"]

        project = "PERSONAL"

        net = float(settings.iloc[0]["Net_Factor"])

        result = self.engine.calc_project_apr(
            members,
            apr,
            net,
            project
        )

        dt = U.fmt_dt(U.now_jst())

        for _, r in result.iterrows():

            self.repo.append_ledger(
                dt,
                project,
                r["PersonName"],
                "APR",
                r["DailyAPR"],
                f"APR:{apr}",
                "",
                r["Line_User_ID"],
                r["LINE_DisplayName"],
                "app"
            )

        self.send_line(project, result)

        st.success("APR処理完了")


    def run(self):

        data = self.store.load()

        page = st.sidebar.radio(
            "MENU",
            [
                AppConfig.PAGE["DASHBOARD"],
                AppConfig.PAGE["APR"],
                AppConfig.PAGE["CASH"],
                AppConfig.PAGE["HELP"]
            ]
        )

        if page == AppConfig.PAGE["DASHBOARD"]:
            self.ui.dashboard(data)

        elif page == AppConfig.PAGE["APR"]:

            run, apr = self.ui.apr_screen(data)

            if run:
                self.execute_apr(apr)

        elif page == AppConfig.PAGE["CASH"]:
            self.ui.cash_screen(self.repo, data["members_df"])

        elif page == AppConfig.PAGE["HELP"]:
            self.ui.help_page()


# =========================================================
# MAIN
# =========================================================
def main():

    st.set_page_config(
        page_title=AppConfig.APP_TITLE,
        page_icon=AppConfig.APP_ICON,
        layout=AppConfig.PAGE_LAYOUT
    )

    AdminAuth.require_login()

    st.title(AppConfig.APP_TITLE)

    st.caption(AdminAuth.current_label())

    app = AppController()

    app.run()


if __name__ == "__main__":
    main()
