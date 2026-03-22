import streamlit as st

# --- デバッグ開始 ---
st.write("🔍 デバッグ: プログラムが起動しました")

try:
    if "admin" not in st.secrets:
        st.error("❌ エラー: Secrets に [admin] セクションが見つかりません。")
        st.stop()
    else:
        st.success("✅ Secrets [admin] 読み込み成功")
        
    if "connections" not in st.secrets:
        st.error("❌ エラー: Secrets に [connections.gsheets] が見つかりません。")
        st.stop()
    else:
        st.success("✅ Secrets [connections] 読み込み成功")
except Exception as e:
    st.error(f"⚠️ Secrets 読み込み中に致命的なエラー: {e}")
    st.stop()

st.write("🚀 次のステップ：認証とサービスのセットアップを開始します...")
# --- デバッグ終了 ---


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
    APP_TITLE = "APR資産運用管理システム Pro"
    APP_ICON = "🏦"
    PAGE_LAYOUT = "wide"
    JST = timezone(timedelta(hours=9), "JST")

    # シート名定義
    SHEET = {
        "SETTINGS": "Settings",
        "MEMBERS": "Members",
        "LEDGER": "Ledger",
        "LINEUSERS": "LineUsers",
        "APR_SUMMARY": "APR_Summary",
        "SMARTVAULT_HISTORY": "SmartVault_History",
        "TRANSACTION_HISTORY": "OCR_Transaction_History", # 追加
    }

    # ヘッダー定義
    HEADERS = {
        "SETTINGS": [
            "Project_Name", "Net_Factor", "IsCompound", "Compound_Timing",
            "Crop_Left_PC", "Crop_Top_PC", "Crop_Right_PC", "Crop_Bottom_PC",
            "Crop_Left_Mob", "Crop_Top_Mob", "Crop_Right_Mob", "Crop_Bottom_Mob",
            "UpdatedAt_JST", "Active"
        ],
        "MEMBERS": ["Project_Name", "PersonName", "Principal", "Line_User_ID", "LINE_DisplayName", "Rank", "IsActive", "CreatedAt_JST", "UpdatedAt_JST"],
        "LEDGER": ["Datetime_JST", "Project_Name", "PersonName", "Type", "Amount", "Note", "Evidence_URL", "Line_User_ID", "LINE_DisplayName", "Source"],
        "LINEUSERS": ["Date", "Time", "Type", "Line_User_ID", "Line_User"],
        "APR_SUMMARY": ["Date_JST", "PersonName", "Total_APR", "APR_Count", "Asset_Ratio", "LINE_DisplayName"],
        "SMARTVAULT_HISTORY": [
            "Datetime_JST", "Project_Name", "Liquidity", "Yesterday_Profit", "APR", "Source_Mode",
            "OCR_Liquidity", "OCR_Yesterday_Profit", "OCR_APR", "Evidence_URL", "Admin_Name", "Admin_Namespace", "Note"
        ],
        "TRANSACTION_HISTORY": [
            "Unique_Key", "Date_Label", "Time_Label", "Type_Label", "Amount_USD", 
            "Token_Amount", "Token_Symbol", "Source_Image", "Source_Project", "OCR_Raw_Text", "CreatedAt_JST"
        ],
    }

    # ページ定義
    PAGE = {
        "DASHBOARD": "📊 ダッシュボード",
        "APR": "📈 APR確定",
        "HISTORY": "📑 履歴OCRスキャン", # 追加
        "CASH": "💸 入金/出金",
        "ADMIN": "⚙️ 管理",
        "HELP": "❓ ヘルプ",
    }

    # スキャン設定（モバイル版履歴リスト用）
    SCAN_SETTINGS = {
        "ROW_START_Y": 0.12,  # 履歴の開始位置(%)
        "ROW_HEIGHT": 0.105,  # 1件あたりの高さ(%)
        "MAX_ROWS": 8,        # 1枚から読み取る最大件数
    }

# =========================================================
# UTILS (強化版)
# =========================================================
class U:
    @staticmethod
    def now_jst() -> datetime: return datetime.now(AppConfig.JST)

    @staticmethod
    def fmt_dt(dt: datetime) -> str: return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def normalize_text(text: str) -> str:
        """OCRのノイズを除去し、半角・小文字に正規化"""
        if not text: return ""
        t = text.translate(str.maketrans("０１２３４５６７８９．＄％：", "0123456789.$%: "))
        t = t.replace("O", "0").replace("o", "0").replace("l", "1").replace("|", "1")
        return re.sub(r"\s+", " ", t).strip()

    @staticmethod
    def parse_transaction_text(raw_text: str) -> Optional[Dict[str, Any]]:
        """1行のOCRテキストから取引情報を抽出"""
        norm = U.normalize_text(raw_text)
        
        # 1. 日付・時間の抽出 (例: 3月 21 at 10:36 am)
        date_match = re.search(r"(\d+月\s*\d+)", norm)
        time_match = re.search(r"(\d+:\d+\s*[ap]m)", norm, re.IGNORECASE)
        
        # 2. USD金額の抽出 (例: $2.33)
        usd_match = re.search(r"\$\s*(\d+[\.,]\d{2})", norm)
        
        # 3. トークン量とシンボルの抽出 (例: 2.33161 USDC)
        token_match = re.search(r"(\d+\.\d+)\s*([A-Z]{3,})", norm)
        
        # 4. タイプ (受け取った/送った等)
        type_label = "Received" if "受け取った" in raw_text or "Receive" in raw_text else "Sent"

        if not (date_match and usd_match): return None

        date_val = date_match.group(1).replace(" ", "")
        time_val = time_match.group(1).lower() if time_match else ""
        usd_val = float(usd_match.group(1).replace(",", ""))
        token_val = float(token_match.group(1)) if token_match else usd_val
        symbol = token_match.group(2) if token_match else "USDC"

        # Unique Key作成: 日付|時間|金額
        unique_key = f"{date_val}_{time_val}_{usd_val}".replace(":", "")

        return {
            "Unique_Key": unique_key,
            "Date_Label": date_val,
            "Time_Label": time_val,
            "Type_Label": type_label,
            "Amount_USD": usd_val,
            "Token_Amount": token_val,
            "Token_Symbol": symbol,
            "OCR_Raw_Text": norm
        }

    @staticmethod
    def crop_by_ratio(img: Image.Image, left: float, top: float, right: float, bottom: float) -> Image.Image:
        w, h = img.size
        return img.crop((int(w * left), int(h * top), int(w * right), int(h * bottom)))

# =========================================================
# EXTERNAL SERVICE (リストスキャン対応)
# =========================================================
class ExternalService:
    @staticmethod
    def ocr_space_request(file_bytes: bytes, engine: int = 2) -> str:
        api_key = st.secrets["ocrspace"]["api_key"]
        try:
            res = requests.post(
                "https://api.ocr.space/parse/image",
                files={"filename": ("image.png", file_bytes)},
                data={"apikey": api_key, "language": "eng", "OCREngine": engine, "scale": True},
                timeout=30
            )
            result = res.json()
            return "\n".join([p.get("ParsedText", "") for p in result.get("ParsedResults", [])])
        except Exception:
            return ""

    @staticmethod
    def scan_transaction_list(file_bytes: bytes) -> List[Dict[str, Any]]:
        """画像を1行ずつスライスしてOCR実行"""
        base_img = Image.open(BytesIO(file_bytes)).convert("RGB")
        conf = AppConfig.SCAN_SETTINGS
        results = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()

        for i in range(conf["MAX_ROWS"]):
            status_text.text(f"スキャン中... {i+1}/{conf['MAX_ROWS']}件目")
            
            # 1行分を切り出し
            top = conf["ROW_START_Y"] + (conf["ROW_HEIGHT"] * i)
            bottom = top + conf["ROW_HEIGHT"]
            if bottom > 1.0: break

            row_img = U.crop_by_ratio(base_img, 0.0, top, 1.0, bottom)
            
            # 前処理 (コントラスト強調)
            row_img = ImageEnhance.Contrast(row_img).enhance(2.0)
            buf = BytesIO()
            row_img.save(buf, format="PNG")
            
            # OCR実行
            raw_text = ExternalService.ocr_space_request(buf.getvalue(), engine=2)
            
            # 解析
            data = U.parse_transaction_text(raw_text)
            if data:
                results.append(data)
            
            progress_bar.progress((i + 1) / conf["MAX_ROWS"])
        
        status_text.text("スキャン完了！")
        return results

    @staticmethod
    def upload_imgbb(file_bytes: bytes) -> Optional[str]:
        try:
            res = requests.post(
                "https://api.imgbb.com/1/upload",
                params={"key": st.secrets["imgbb"]["api_key"]},
                files={"image": file_bytes},
                timeout=30
            )
            return res.json()["data"]["url"]
        except: return None

# =========================================================
# UI - HISTORY SCAN PAGE
# =========================================================
class HistoryUI:
    def __init__(self, repo: Repository):
        self.repo = repo

    def render(self):
        st.subheader("📑 取引履歴OCRスキャン")
        st.info("受け取り履歴のスクリーンショットをアップロードして、自動的に明細を抽出します。")

        uploaded = st.file_uploader("履歴画像をアップロード", type=["png", "jpg", "jpeg"])
        project = st.selectbox("対象プロジェクト", self.repo.active_projects(st.session_state["settings_df"]))

        if uploaded and st.button("スキャン開始"):
            file_bytes = uploaded.getvalue()
            
            # スキャン実行
            items = ExternalService.scan_transaction_list(file_bytes)
            
            if not items:
                st.error("取引データを検出できませんでした。画像の範囲や画質を確認してください。")
                return

            # 結果表示 & 保存準備
            st.write(f"### 検出結果 ({len(items)}件)")
            df_preview = pd.DataFrame(items)
            st.dataframe(df_preview.drop(columns=["OCR_Raw_Text"]), use_container_width=True)

            if st.button("スプレッドシートへ保存"):
                img_url = ExternalService.upload_imgbb(file_bytes) or ""
                now = U.fmt_dt(U.now_jst())
                
                # 既存のキーを取得（重複チェック用）
                hist_df = self.repo.gs.load_df("TRANSACTION_HISTORY")
                existing_keys = set(hist_df["Unique_Key"].tolist()) if not hist_df.empty else set()

                added_count = 0
                for item in items:
                    if item["Unique_Key"] in existing_keys: continue
                    
                    row = [
                        item["Unique_Key"], item["Date_Label"], item["Time_Label"], item["Type_Label"],
                        item["Amount_USD"], item["Token_Amount"], item["Token_Symbol"],
                        img_url, project, item["OCR_Raw_Text"], now
                    ]
                    self.repo.gs.append_row("TRANSACTION_HISTORY", row)
                    added_count += 1
                
                st.success(f"保存完了！ 新規登録: {added_count}件 (重複スキップ: {len(items)-added_count}件)")
