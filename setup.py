import os

# プロジェクトのルートディレクトリ名
project_name = "apr_system_pro"
os.makedirs(f"{project_name}/.github/workflows", exist_ok=True)
os.makedirs(f"{project_name}/.streamlit", exist_ok=True)

# 1. requirements.txt の内容
requirements_content = """streamlit
pandas
requests
Pillow
gspread
google-auth
"""

# 2. GitHub Actions (YAML) の内容
github_action_content = """name: Python Build and Lint

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt

    - name: Lint with flake8
      run: |
        pip install flake8
        flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
"""

# 3. .gitignore の内容
gitignore_content = """.streamlit/secrets.toml
__pycache__/
*.pyc
.env
"""

# ファイル書き出し
files = {
    "requirements.txt": requirements_content,
    ".github/workflows/streamlit-app.yml": github_action_content,
    ".gitignore": gitignore_content,
}

for path, content in files.items():
    with open(f"{project_name}/{path}", "w", encoding="utf-8") as f:
        f.write(content)

print(f"✅ '{project_name}' フォルダに必要なファイルを作成しました。")
print("あとは、先ほどの 'main.py' をこのフォルダに入れて、GitHubにPushするだけです！")
