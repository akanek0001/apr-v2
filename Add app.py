# =========================================================
# IMPORT
# =========================================================

import streamlit as st
import pandas as pd
import requests
import datetime
import pytz
import re
from typing import Dict, List, Tuple, Any

# =========================================================
# CONFIG
# =========================================================

class AppConfig:

    FACTOR = {
        "MASTER": 0.67,
        "ELITE": 0.60
    }

    TYPE = {
        "APR": "APR",
        "LINE": "LINE",
        "CASHIN": "CASHIN",
        "CASHOUT": "CASHOUT"
    }

    COMPOUND = {
        "DAILY": "daily",
        "MONTHLY": "monthly",
        "NONE": "none"
    }

    TZ = "Asia/Tokyo"

# =========================================================
# AUTH
# =========================================================

class AdminAuth:

    @staticmethod
    def require_login():

        if "admin_namespace" not in st.session_state:

            st.title("管理者ログイン")

            namespace = st.selectbox(
                "管理者",
                ["A", "B", "C", "D"]
            )

            pin = st.text_input("PIN", type="password")

            if st.button("ログイン"):

                if pin == st.secrets["admin"]["pin"]:
                    st.session_state["admin_namespace"] = namespace
                    st.session_state["admin_name"] = f"Admin{namespace}"
                    st.rerun()
                else:
                    st.error("PINが違います")

            st.stop()

    @staticmethod
    def current_namespace():
        return st.session_state.get("admin_namespace", "A")

    @staticmethod
    def current_name():
        return st.session_state.get("admin_name", "AdminA")









# =========================================================
# UTILITY
# =========================================================

class U:

    @staticmethod
    def now_jst():
        return datetime.datetime.now(pytz.timezone(AppConfig.TZ))

    @staticmethod
    def now_jst_str():
        return U.now_jst().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def fmt_usd(v):
        try:
            return f"${float(v):,.2f}"
        except:
            return "$0"

    @staticmethod
    def apr_val(v):
        try:
            return float(v)
        except:
            return 0.0

    @staticmethod
    def to_f(v):
        try:
            return float(str(v).replace(",", ""))
        except:
            return 0.0


# =========================================================
# LINE SERVICE
# =========================================================

class ExternalService:

    @staticmethod
    def get_line_token(namespace):

        tokens = st.secrets["line"]["tokens"]

        if namespace in tokens:
            return tokens[namespace]

        return ""

    @staticmethod
    def send_line_push(token, user_id, text, image_url=None):

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
# GSHEET
# =========================================================

class GSheetService:

    def __init__(self, namespace):

        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = Credentials.from_service_account_info(
            st.secrets["connections"]["gsheets"]["credentials"],
            scopes=scopes
        )

        self.gc = gspread.authorize(creds)

        self.sheet = self.gc.open_by_key(
            st.secrets["spreadsheet"]["id"]
        )

        self.namespace = namespace

    def read(self, name):

        ws = self.sheet.worksheet(f"{name}__{self.namespace}")

        return pd.DataFrame(ws.get_all_records())

    def append(self, name, row):

        ws = self.sheet.worksheet(f"{name}__{self.namespace}")

        ws.append_row(row)

    def write(self, name, df):

        ws = self.sheet.worksheet(f"{name}__{self.namespace}")

        ws.clear()

        ws.update(
            [df.columns.values.tolist()] +
            df.values.tolist()
        )


# =========================================================
# REPOSITORY
# =========================================================

class Repository:

    def __init__(self, gs):

        self.gs = gs

    def load_all(self):

        return {
            "settings": self.gs.read("Settings"),
            "members": self.gs.read("Members"),
            "ledger": self.gs.read("Ledger"),
            "apr_summary": self.gs.read("APR_Summary")
        }

    def append_ledger(self, row):

        self.gs.append("Ledger", row)

    def write_members(self, df):

        self.gs.write("Members", df)

    def write_settings(self, df):

        self.gs.write("Settings", df)


# =========================================================
# FINANCE
# =========================================================

class FinanceEngine:

    @staticmethod
    def calc_project_apr(members_df, apr, factor, project):

        mem = members_df[
            members_df["Project_Name"] == project
        ].copy()

        mem["DailyAPR"] = mem["Principal"] * (apr/100) * factor

        return mem











# =========================================================
# GSHEET
# =========================================================

class GSheetService:

    def __init__(self, namespace):

        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = Credentials.from_service_account_info(
            st.secrets["connections"]["gsheets"]["credentials"],
            scopes=scopes
        )

        self.gc = gspread.authorize(creds)

        self.sheet = self.gc.open_by_key(
            st.secrets["spreadsheet"]["id"]
        )

        self.namespace = namespace

    def read(self, name):

        ws = self.sheet.worksheet(f"{name}__{self.namespace}")

        return pd.DataFrame(ws.get_all_records())

    def append(self, name, row):

        ws = self.sheet.worksheet(f"{name}__{self.namespace}")

        ws.append_row(row)

    def write(self, name, df):

        ws = self.sheet.worksheet(f"{name}__{self.namespace}")

        ws.clear()

        ws.update(
            [df.columns.values.tolist()] +
            df.values.tolist()
        )


# =========================================================
# REPOSITORY
# =========================================================

class Repository:

    def __init__(self, gs):

        self.gs = gs

    def load_all(self):

        return {
            "settings": self.gs.read("Settings"),
            "members": self.gs.read("Members"),
            "ledger": self.gs.read("Ledger"),
            "apr_summary": self.gs.read("APR_Summary")
        }

    def append_ledger(self, row):

        self.gs.append("Ledger", row)

    def write_members(self, df):

        self.gs.write("Members", df)

    def write_settings(self, df):

        self.gs.write("Settings", df)


# =========================================================
# FINANCE
# =========================================================

class FinanceEngine:

    @staticmethod
    def calc_project_apr(members_df, apr, factor, project):

        mem = members_df[
            members_df["Project_Name"] == project
        ].copy()

        mem["DailyAPR"] = mem["Principal"] * (apr/100) * factor

        return mem
