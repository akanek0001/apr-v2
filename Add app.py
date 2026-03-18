from __future__ import annotations

# =========================================================
# IMPORT
# =========================================================

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set

import json
import re
import base64
from io import BytesIO

import pandas as pd
import requests
import streamlit as st

from PIL import Image, ImageEnhance, ImageFilter, ImageOps

import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# CONFIG
# =========================================================

class AppConfig:

    APP_TITLE = "APR資産運用管理システム"
    APP_ICON = "🏦"
    PAGE_LAYOUT = "wide"

    JST = timezone(timedelta(hours=9), "JST")

    STATUS = {
        "ON": "🟢運用中",
        "OFF": "🔴停止"
    }

    TYPE = {
        "APR": "APR",
        "LINE": "LINE",
        "DEPOSIT": "DEPOSIT",
        "WITHDRAW": "WITHDRAW"
    }

    RANK = {
        "MASTER": "Master",
        "ELITE": "Elite"
    }

    FACTOR = {
        "MASTER": 0.67,
        "ELITE": 0.60
    }

    PROJECT = {
        "PERSONAL": "PERSONAL"
    }

    COMPOUND = {
        "DAILY": "daily",
        "MONTHLY": "monthly",
        "NONE": "none"
    }

    PAGE = {
        "DASHBOARD": "Dashboard",
        "APR": "APR",
        "CASH": "Cash",
        "ADMIN": "Admin",
        "HELP": "Help"
    }

    SOURCE = {
        "APP": "APP"
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
    def to_f(v: Any) -> float:
        try:
            return float(v)
        except:
            return 0.0

    @staticmethod
    def to_num_series(s: pd.Series) -> pd.Series:
        return pd.to_numeric(s, errors="coerce").fillna(0)

    @staticmethod
    def fmt_usd(x: float) -> str:
        return f"${float(x):,.2f}"

    @staticmethod
    def truthy(v: Any) -> bool:
        return str(v).lower() in ["true", "1", "yes"]

    @staticmethod
    def normalize_rank(v: str) -> str:
        v = str(v).strip().lower()
        if v == "elite":
            return "Elite"
        return "Master"

    @staticmethod
    def rank_factor(rank: str) -> float:
        return AppConfig.FACTOR["MASTER"] if rank == "Master" else AppConfig.FACTOR["ELITE"]

    @staticmethod
    def apr_val(v: Any) -> float:
        try:
            return float(v)
        except:
            return 0.0


# =========================================================
# ADMIN AUTH
# =========================================================

class AdminAuth:

    @staticmethod
    def require_login():

        if "admin_ok" not in st.session_state:
            st.session_state["admin_ok"] = False

        if st.session_state["admin_ok"]:
            return

        st.title("Admin Login")

        pin = st.text_input("PIN", type="password")

        if st.button("Login"):
            valid_pin = (
                st.secrets.get("admin", {}).get("pin")
                or st.secrets.get("admin", {}).get("users", [{}])[0].get("pin")
            )

            if pin == valid_pin:
                st.session_state["admin_ok"] = True
                st.session_state["admin_namespace"] = "A"
                st.session_state["admin_name"] = "Admin"
                st.rerun()

        st.stop()

    @staticmethod
    def current_namespace() -> str:
        return st.session_state.get("admin_namespace", "A")

    @staticmethod
    def current_name() -> str:
        return st.session_state.get("admin_name", "Admin")

    @staticmethod
    def current_label() -> str:
        return f'{AdminAuth.current_name()} ({AdminAuth.current_namespace()})'


# =========================================================
# GOOGLE SHEETS SERVICE
# =========================================================

class GSheetService:

    def __init__(self, spreadsheet_id: str, namespace: str):

        creds_dict = st.secrets["connections"]["gsheets"]["credentials"]

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)

        client = gspread.authorize(creds)

        self.spreadsheet = client.open_by_key(spreadsheet_id)

        self.namespace = namespace

    def ws(self, name: str):
        return self.spreadsheet.worksheet(name)

    def load_df(self, name: str) -> pd.DataFrame:

        ws = self.ws(name)

        data = ws.get_all_records()

        return pd.DataFrame(data)

    def append_row(self, name: str, row: List[Any]):

        ws = self.ws(name)

        ws.append_row(row)

    def write_df(self, name: str, df: pd.DataFrame):

        ws = self.ws(name)

        ws.clear()

        ws.update([df.columns.values.tolist()] + df.values.tolist())


# =========================================================
# LINE SERVICE
# =========================================================

class LineService:

    @staticmethod
    def get_token(namespace: str) -> str:

        tokens = st.secrets["line"]["tokens"]

        if namespace in tokens:
            return tokens[namespace]

        return ""

    @staticmethod
    def push(token, user_id, text, image_url=None):

        url = "https://api.line.me/v2/bot/message/push"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        messages = [{"type": "text", "text": text}]

        if image_url:
            messages.append({
                "type": "image",
                "originalContentUrl": image_url,
                "previewImageUrl": image_url
            })

        data = {
            "to": user_id,
            "messages": messages
        }

        r = requests.post(url, headers=headers, json=data)

        return r.status_code


# =========================================================
# FINANCE ENGINE
# =========================================================

class FinanceEngine:

    def calc_apr(self, members_df: pd.DataFrame, apr: float) -> pd.DataFrame:

        df = members_df.copy()

        df["Principal"] = U.to_num_series(df["Principal"])

        df["Rank"] = df["Rank"].apply(U.normalize_rank)

        df["Factor"] = df["Rank"].apply(U.rank_factor)

        df["DailyAPR"] = (df["Principal"] * (apr/100) * df["Factor"]) / 365

        return df
        # =========================================================
# REPOSITORY
# =========================================================

class Repository:

    def __init__(self, gs: GSheetService):
        self.gs = gs

    def load_settings(self) -> pd.DataFrame:
        try:
            return self.gs.load_df("Settings")
        except Exception:
            return pd.DataFrame(columns=[
                "Project_Name", "Net_Factor", "IsCompound", "Compound_Timing",
                "Active"
            ])

    def load_members(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("Members")
        except Exception:
            return pd.DataFrame(columns=[
                "Project_Name", "PersonName", "Principal", "Line_User_ID",
                "LINE_DisplayName", "Rank", "IsActive", "CreatedAt_JST", "UpdatedAt_JST"
            ])

        if df.empty:
            return df

        if "Principal" in df.columns:
            df["Principal"] = U.to_num_series(df["Principal"])
        if "Rank" in df.columns:
            df["Rank"] = df["Rank"].apply(U.normalize_rank)
        if "IsActive" in df.columns:
            df["IsActive"] = df["IsActive"].apply(U.truthy)

        return df

    def load_ledger(self) -> pd.DataFrame:
        try:
            df = self.gs.load_df("Ledger")
        except Exception:
            return pd.DataFrame(columns=[
                "Datetime_JST", "Project_Name", "PersonName", "Type", "Amount",
                "Note", "Evidence_URL", "Line_User_ID", "LINE_DisplayName", "Source"
            ])

        if df.empty:
            return df

        if "Amount" in df.columns:
            df["Amount"] = U.to_num_series(df["Amount"])

        return df

    def load_lineusers(self) -> pd.DataFrame:
        try:
            return self.gs.load_df("LineUsers")
        except Exception:
            return pd.DataFrame(columns=["Date", "Time", "Type", "Line_User_ID", "Line_User"])

    def write_members(self, df: pd.DataFrame):
        self.gs.write_df("Members", df)

    def write_settings(self, df: pd.DataFrame):
        self.gs.write_df("Settings", df)

    def append_ledger(
        self,
        dt_jst: str,
        project: str,
        person_name: str,
        typ: str,
        amount: float,
        note: str = "",
        evidence_url: str = "",
        line_user_id: str = "",
        line_display_name: str = "",
        source: str = AppConfig.SOURCE["APP"]
    ):
        self.gs.append_row("Ledger", [
            dt_jst,
            project,
            person_name,
            typ,
            amount,
            note,
            evidence_url,
            line_user_id,
            line_display_name,
            source
        ])

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
        note: str = ""
    ):
        self.gs.append_row("SmartVault_History", [
            dt_jst,
            project,
            liquidity,
            yesterday_profit,
            apr,
            source_mode,
            ocr_liquidity if ocr_liquidity is not None else "",
            ocr_yesterday_profit if ocr_yesterday_profit is not None else "",
            ocr_apr if ocr_apr is not None else "",
            evidence_url,
            admin_name,
            admin_namespace,
            note
        ])

    def active_projects(self, settings_df: pd.DataFrame) -> List[str]:
        if settings_df.empty or "Project_Name" not in settings_df.columns:
            return []
        if "Active" not in settings_df.columns:
            return settings_df["Project_Name"].dropna().astype(str).unique().tolist()

        active = settings_df.copy()
        active["Active"] = active["Active"].apply(U.truthy)
        return active.loc[active["Active"] == True, "Project_Name"].dropna().astype(str).unique().tolist()

    def project_members_active(self, members_df: pd.DataFrame, project: str) -> pd.DataFrame:
        if members_df.empty:
            return members_df.copy()

        df = members_df.copy()
        if "IsActive" in df.columns:
            df["IsActive"] = df["IsActive"].apply(U.truthy)
            df = df[df["IsActive"] == True]

        return df[df["Project_Name"].astype(str) == str(project)].copy().reset_index(drop=True)


# =========================================================
# OCR SERVICE
# =========================================================

class OCRService:

    @staticmethod
    def call_ocr_space(img: Image.Image) -> str:
        api_key = st.secrets["ocr"]["api_key"]

        buffer = BytesIO()
        img.save(buffer, format="PNG")
        buffer.seek(0)

        payload = {
            "apikey": api_key,
            "language": "eng",
            "isOverlayRequired": False
        }

        files = {
            "file": ("ocr.png", buffer, "image/png")
        }

        r = requests.post("https://api.ocr.space/parse/image", data=payload, files=files)

        if r.status_code != 200:
            return ""

        try:
            data = r.json()
            return data["ParsedResults"][0]["ParsedText"]
        except Exception:
            return ""

    @staticmethod
    def parse_apr(text: str) -> Optional[float]:
        if not text:
            return None

        m = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return None

        nums = re.findall(r"\d+(?:\.\d+)?", text)
        if not nums:
            return None

        vals = []
        for n in nums:
            try:
                vals.append(float(n))
            except Exception:
                pass

        if not vals:
            return None

        # APR候補として 0-300 の範囲を優先
        vals = [v for v in vals if 0 <= v <= 300]
        if not vals:
            return None

        return max(vals)

    @staticmethod
    def parse_money(text: str) -> Optional[float]:
        if not text:
            return None

        nums = re.findall(r"\d{1,3}(?:,\d{3})*(?:\.\d+)?", text)
        vals = []
        for n in nums:
            try:
                vals.append(float(n.replace(",", "")))
            except Exception:
                pass

        if not vals:
            return None

        return max(vals)


# =========================================================
# IMGBB SERVICE
# =========================================================

class ImgBBService:

    @staticmethod
    def upload(img: Image.Image) -> Optional[str]:
        api_key = st.secrets["imgbb"]["api_key"]

        buffer = BytesIO()
        img.save(buffer, format="PNG")
        encoded = base64.b64encode(buffer.getvalue()).decode()

        payload = {
            "key": api_key,
            "image": encoded
        }

        r = requests.post("https://api.imgbb.com/1/upload", data=payload)

        if r.status_code != 200:
            return None

        try:
            return r.json()["data"]["url"]
        except Exception:
            return None


# =========================================================
# APR EXECUTION SERVICE
# =========================================================

class APRExecutionService:

    def __init__(self, repo: Repository, engine: FinanceEngine, namespace: str, admin_name: str):
        self.repo = repo
        self.engine = engine
        self.namespace = namespace
        self.admin_name = admin_name

    def build_line_message(
        self,
        project: str,
        person: str,
        reward: float,
        apr: float,
        liquidity: float,
        yesterday_profit: float,
        principal: float
    ) -> str:
        return (
            "📊 SmartVault APR Report\n\n"
            f"Project\n{project}\n\n"
            f"Member\n{person}\n\n"
            f"Liquidity\n{U.fmt_usd(liquidity)}\n\n"
            f"Yesterday Profit\n{U.fmt_usd(yesterday_profit)}\n\n"
            f"APR\n{apr:.2f}%\n\n"
            f"Today Reward\n{U.fmt_usd(reward)}\n\n"
            f"Current Principal\n{U.fmt_usd(principal)}"
        )

    def execute_apr(
        self,
        settings_df: pd.DataFrame,
        members_df: pd.DataFrame,
        project: str,
        apr: float,
        liquidity: float,
        yesterday_profit: float,
        evidence_url: str = ""
    ) -> Tuple[int, int]:
        if settings_df.empty:
            return 0, 0

        project_row = settings_df[settings_df["Project_Name"].astype(str) == str(project)].iloc[0]

        net_factor = U.to_f(project_row["Net_Factor"]) if "Net_Factor" in project_row else AppConfig.FACTOR["MASTER"]
        if net_factor <= 0:
            net_factor = AppConfig.FACTOR["MASTER"]

        compound_timing = str(project_row.get("Compound_Timing", AppConfig.COMPOUND["NONE"])).strip().lower()

        mem = self.repo.project_members_active(members_df, project)
        if mem.empty:
            return 0, 0

        calc_df = self.engine.calc_apr(mem, apr)

        # GROUPならNet_Factor反映
        if str(project).upper() != AppConfig.PROJECT["PERSONAL"]:
            calc_df["DailyAPR"] = calc_df["DailyAPR"] * net_factor

        token = LineService.get_token(self.namespace)
        dt_jst = U.fmt_dt(U.now_jst())

        apr_count = 0
        line_count = 0

        for _, row in calc_df.iterrows():
            person = str(row["PersonName"]).strip()
            reward = float(U.to_f(row["DailyAPR"]))
            uid = str(row.get("Line_User_ID", "")).strip()
            disp = str(row.get("LINE_DisplayName", "")).strip()
            principal = float(U.to_f(row.get("Principal", 0)))

            self.repo.append_ledger(
                dt_jst=dt_jst,
                project=project,
                person_name=person,
                typ=AppConfig.TYPE["APR"],
                amount=reward,
                note=f"APR:{apr}%",
                evidence_url=evidence_url,
                line_user_id=uid,
                line_display_name=disp,
                source=AppConfig.SOURCE["APP"]
            )
            apr_count += 1

            # daily 複利
            if compound_timing == AppConfig.COMPOUND["DAILY"]:
                idx_list = members_df[
                    (members_df["Project_Name"].astype(str) == str(project))
                    & (members_df["PersonName"].astype(str) == person)
                ].index.tolist()

                for idx in idx_list:
                    members_df.loc[idx, "Principal"] = float(U.to_f(members_df.loc[idx, "Principal"])) + reward
                    members_df.loc[idx, "UpdatedAt_JST"] = dt_jst

            msg = self.build_line_message(
                project=project,
                person=person,
                reward=reward,
                apr=apr,
                liquidity=liquidity,
                yesterday_profit=yesterday_profit,
                principal=principal
            )

            if uid and token:
                code = LineService.push(token, uid, msg, evidence_url if evidence_url else None)
                line_note = f"HTTP:{code}, APR:{apr}%"
            else:
                line_note = "LINE未送信"

            self.repo.append_ledger(
                dt_jst=dt_jst,
                project=project,
                person_name=person,
                typ=AppConfig.TYPE["LINE"],
                amount=0,
                note=line_note,
                evidence_url=evidence_url,
                line_user_id=uid,
                line_display_name=disp,
                source=AppConfig.SOURCE["APP"]
            )
            line_count += 1

        self.repo.write_members(members_df)

        self.repo.append_smartvault_history(
            dt_jst=dt_jst,
            project=project,
            liquidity=liquidity,
            yesterday_profit=yesterday_profit,
            apr=apr,
            source_mode="manual" if not evidence_url else "ocr+manual",
            ocr_liquidity=None,
            ocr_yesterday_profit=None,
            ocr_apr=None,
            evidence_url=evidence_url,
            admin_name=self.admin_name,
            admin_namespace=self.namespace,
            note="APR確定"
        )

        return apr_count, line_count


# =========================================================
# UI
# =========================================================

class AppUI:

    def __init__(self, repo: Repository, engine: FinanceEngine, executor: APRExecutionService):
        self.repo = repo
        self.engine = engine
        self.executor = executor

    def render_dashboard(self, members_df: pd.DataFrame, ledger_df: pd.DataFrame):
        st.subheader("📊 Dashboard")

        total_assets = 0.0
        if not members_df.empty and "Principal" in members_df.columns:
            total_assets = float(U.to_num_series(members_df["Principal"]).sum())

        total_apr = 0.0
        if not ledger_df.empty and "Type" in ledger_df.columns:
            apr_df = ledger_df[ledger_df["Type"].astype(str) == AppConfig.TYPE["APR"]].copy()
            if not apr_df.empty:
                total_apr = float(U.to_num_series(apr_df["Amount"]).sum())

        c1, c2 = st.columns(2)
        c1.metric("総資産", U.fmt_usd(total_assets))
        c2.metric("累計APR", U.fmt_usd(total_apr))

        st.divider()

        st.markdown("### Members")
        if members_df.empty:
            st.info("Members がありません")
        else:
            view = members_df.copy()
            if "Principal" in view.columns:
                view["Principal"] = U.to_num_series(view["Principal"]).apply(U.fmt_usd)
            st.dataframe(view, use_container_width=True, hide_index=True)

        st.divider()

        st.markdown("### Ledger")
        if ledger_df.empty:
            st.info("Ledger がありません")
        else:
            view = ledger_df.copy()
            if "Amount" in view.columns:
                view["Amount"] = U.to_num_series(view["Amount"]).apply(U.fmt_usd)
            st.dataframe(view.sort_values("Datetime_JST", ascending=False), use_container_width=True, hide_index=True)

    def render_apr(self, settings_df: pd.DataFrame, members_df: pd.DataFrame):
        st.subheader("📈 APR")

        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効なプロジェクトがありません")
            return

        project = st.selectbox("プロジェクト", projects, key="apr_project")

        c1, c2, c3 = st.columns(3)
        with c1:
            liquidity_raw = st.text_input("流動性", value=st.session_state.get("sv_liquidity", ""))
        with c2:
            profit_raw = st.text_input("昨日の収益", value=st.session_state.get("sv_profit", ""))
        with c3:
            apr_raw = st.text_input("APR %", value=st.session_state.get("sv_apr", ""))

        liquidity = U.to_f(liquidity_raw)
        yesterday_profit = U.to_f(profit_raw)
        apr = U.apr_val(apr_raw)

        st.info(
            f"流動性 = {U.fmt_usd(liquidity)} / "
            f"昨日の収益 = {U.fmt_usd(yesterday_profit)} / "
            f"APR = {apr:.4f}%"
        )

        uploaded = st.file_uploader("エビデンス画像", type=["png", "jpg", "jpeg"], key="apr_file")

        if uploaded is not None and st.button("OCR読取"):
            try:
                img = Image.open(uploaded).convert("RGB")
                text = OCRService.call_ocr_space(img)

                st.text_area("OCR結果", text, height=180)

                found_apr = OCRService.parse_apr(text)
                if found_apr is not None:
                    st.session_state["sv_apr"] = f"{found_apr:.4f}"
                    st.rerun()

            except Exception as e:
                st.error(f"OCRエラー: {e}")

        mem = self.repo.project_members_active(members_df, project)
        if mem.empty:
            st.info("運用中メンバーがいません")
            return

        preview = self.engine.calc_apr(mem, apr)
        if str(project).upper() != AppConfig.PROJECT["PERSONAL"]:
            row = settings_df[settings_df["Project_Name"].astype(str) == str(project)].iloc[0]
            net_factor = U.to_f(row.get("Net_Factor", AppConfig.FACTOR["MASTER"]))
            if net_factor <= 0:
                net_factor = AppConfig.FACTOR["MASTER"]
            preview["DailyAPR"] = preview["DailyAPR"] * net_factor

        st.markdown("### 配当プレビュー")
        view = preview.copy()
        view["Principal"] = U.to_num_series(view["Principal"]).apply(U.fmt_usd)
        view["DailyAPR"] = U.to_num_series(view["DailyAPR"]).apply(U.fmt_usd)
        st.dataframe(
            view[["PersonName", "Rank", "Factor", "Principal", "DailyAPR", "LINE_DisplayName"]],
            use_container_width=True,
            hide_index=True
        )

        if st.button("APR確定して送信"):
            try:
                if apr <= 0:
                    st.warning("APRを入力してください")
                    return

                evidence_url = ""
                if uploaded is not None:
                    img = Image.open(uploaded).convert("RGB")
                    evidence_url = ImgBBService.upload(img) or ""

                apr_count, line_count = self.executor.execute_apr(
                    settings_df=settings_df,
                    members_df=members_df,
                    project=project,
                    apr=apr,
                    liquidity=liquidity,
                    yesterday_profit=yesterday_profit,
                    evidence_url=evidence_url
                )

                st.success(f"APR記録: {apr_count}件 / LINE記録: {line_count}件")
                st.rerun()

            except Exception as e:
                st.error(f"APR実行エラー: {e}")


class ExtraUI:

    def __init__(self, repo: Repository):
        self.repo = repo

    def render_cash(self, settings_df: pd.DataFrame, members_df: pd.DataFrame):
        st.subheader("💸 Cash")

        projects = self.repo.active_projects(settings_df)
        if not projects:
            st.warning("有効なプロジェクトがありません")
            return

        project = st.selectbox("プロジェクト", projects, key="cash_project")
        mem = self.repo.project_members_active(members_df, project)
        if mem.empty:
            st.info("運用中メンバーがいません")
            return

        person = st.selectbox("メンバー", mem["PersonName"].tolist(), key="cash_person")
        row = mem[mem["PersonName"] == person].iloc[0]
        current = float(U.to_f(row["Principal"]))

        typ = st.selectbox("種別", [AppConfig.TYPE["DEPOSIT"], AppConfig.TYPE["WITHDRAW"]], key="cash_type")
        amount = st.number_input("金額", min_value=0.0, step=100.0, key="cash_amount")
        note = st.text_input("メモ", key="cash_note")

        if st.button("保存", key="cash_save"):
            if amount <= 0:
                st.warning("金額を入力してください")
                return

            new_balance = current + amount if typ == AppConfig.TYPE["DEPOSIT"] else current - amount
            if new_balance < 0:
                st.error("残高不足です")
                return

            idx_list = members_df[
                (members_df["Project_Name"].astype(str) == str(project))
                & (members_df["PersonName"].astype(str) == str(person))
            ].index.tolist()

            dt_jst = U.fmt_dt(U.now_jst())
            for idx in idx_list:
                members_df.loc[idx, "Principal"] = new_balance
                members_df.loc[idx, "UpdatedAt_JST"] = dt_jst

            self.repo.write_members(members_df)
            self.repo.append_ledger(
                dt_jst,
                project,
                person,
                typ,
                amount,
                note,
                "",
                str(row.get("Line_User_ID", "")),
                str(row.get("LINE_DisplayName", "")),
                AppConfig.SOURCE["APP"]
            )

            st.success("保存しました")
            st.rerun()

    def render_admin(self, settings_df: pd.DataFrame, members_df: pd.DataFrame):
        st.subheader("⚙️ Admin")

        tab1, tab2 = st.tabs(["Settings", "Members"])

        with tab1:
            edited_settings = st.data_editor(settings_df, use_container_width=True, hide_index=True, num_rows="dynamic")
            if st.button("Settings保存"):
                self.repo.write_settings(edited_settings)
                st.success("Settings を保存しました")
                st.rerun()

        with tab2:
            edited_members = st.data_editor(members_df, use_container_width=True, hide_index=True, num_rows="dynamic")
            if st.button("Members保存"):
                self.repo.write_members(edited_members)
                st.success("Members を保存しました")
                st.rerun()

    def render_help(self):
        st.subheader("❓ Help")
        st.markdown(
            """
- APR計算
- LINE送信
- 入金 / 出金
- OCRによるAPR候補取得
- Google Sheets保存
"""
        )


# =========================================================
# CONTROLLER
# =========================================================

class App:

    def __init__(self):
        spreadsheet_id = st.secrets["connections"]["gsheets"]["spreadsheet"]
        namespace = AdminAuth.current_namespace()
        admin_name = AdminAuth.current_name()

        self.gs = GSheetService(spreadsheet_id, namespace)
        self.repo = Repository(self.gs)
        self.engine = FinanceEngine()
        self.executor = APRExecutionService(self.repo, self.engine, namespace, admin_name)
        self.ui = AppUI(self.repo, self.engine, self.executor)
        self.extra_ui = ExtraUI(self.repo)

    def run(self):
        st.title(AppConfig.APP_TITLE)
        st.sidebar.caption(f"👤 {AdminAuth.current_label()}")

        settings_df = self.repo.load_settings()
        members_df = self.repo.load_members()
        ledger_df = self.repo.load_ledger()

        menu = st.sidebar.radio(
            "メニュー",
            [
                AppConfig.PAGE["DASHBOARD"],
                AppConfig.PAGE["APR"],
                AppConfig.PAGE["CASH"],
                AppConfig.PAGE["ADMIN"],
                AppConfig.PAGE["HELP"]
            ]
        )

        if st.sidebar.button("🔓 ログアウト", use_container_width=True):
            st.session_state["admin_ok"] = False
            st.rerun()

        if menu == AppConfig.PAGE["DASHBOARD"]:
            self.ui.render_dashboard(members_df, ledger_df)
        elif menu == AppConfig.PAGE["APR"]:
            self.ui.render_apr(settings_df, members_df)
        elif menu == AppConfig.PAGE["CASH"]:
            self.extra_ui.render_cash(settings_df, members_df)
        elif menu == AppConfig.PAGE["ADMIN"]:
            self.extra_ui.render_admin(settings_df, members_df)
        else:
            self.extra_ui.render_help()


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
    App().run()


if __name__ == "__main__":
    main()
