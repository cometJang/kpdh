import json

# Jupyter Notebook 구조 생성
notebook = {
    "cells": [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# 판매 채널 데이터 전처리: Wide → Long 변환 (영문 버전)\n",
                "\n",
                "**목표:** 연도별로 분산된 판매 데이터를 하나의 테이블로 통합"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 1: 라이브러리 import & 파일 읽기"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "import pandas as pd\n",
                "\n",
                "# 파일 읽기 (실제 파일 경로로 수정)\n",
                "df = pd.read_excel('파일명.xlsx')\n",
                "\n",
                "# 원본 확인\n",
                "print(\"원본 데이터:\")\n",
                "display(df)\n",
                "print(f\"\\n원본 shape: {df.shape}\")\n",
                "print(f\"\\n컬럼: {df.columns.tolist()}\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 2: '계' 행 제거"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# '계' 행 제거 (합계는 나중에 계산 가능)\n",
                "print(\"제거 전:\")\n",
                "display(df)\n",
                "\n",
                "df = df[df['구분'] != '계'].copy()\n",
                "\n",
                "print(\"\\n'계' 제거 후:\")\n",
                "display(df)\n",
                "print(f\"제거 후 shape: {df.shape}\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 3: 2021년 데이터만 변환 테스트"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 2021년 데이터만 먼저 변환 테스트\n",
                "temp_2021 = df[['구분', '2021년', '2021년_구성비']].copy()\n",
                "temp_2021.columns = ['channel', 'amount', 'ratio']\n",
                "temp_2021['year'] = 2021\n",
                "\n",
                "print(\"2021년 데이터 변환 결과:\")\n",
                "display(temp_2021)\n",
                "print(f\"\\nShape: {temp_2021.shape}\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 4: 모든 연도 데이터 변환 & 합치기"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 모든 연도 처리\n",
                "dfs = []\n",
                "for year in [2021, 2022, 2023, 2024, 2025]:\n",
                "    temp = df[['구분', f'{year}년', f'{year}년_구성비']].copy()\n",
                "    temp.columns = ['channel', 'amount', 'ratio']\n",
                "    temp['year'] = year\n",
                "    dfs.append(temp)\n",
                "    print(f\"{year}년 데이터 추가: {temp.shape}\")\n",
                "\n",
                "# 모든 연도 합치기\n",
                "df_long = pd.concat(dfs, ignore_index=True)\n",
                "\n",
                "print(\"\\n합친 결과:\")\n",
                "display(df_long.head(10))\n",
                "print(f\"\\n전체 shape: {df_long.shape}\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 5: 컬럼 순서 정리"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 컬럼 순서 정리 (year를 맨 앞으로)\n",
                "df_long = df_long[['year', 'channel', 'amount', 'ratio']]\n",
                "\n",
                "print(\"컬럼 순서 정리 후:\")\n",
                "display(df_long.head(10))\n",
                "print(f\"\\n컬럼: {df_long.columns.tolist()}\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 6: 데이터 타입 변환"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 변환 전 데이터 타입 확인\n",
                "print(\"변환 전 데이터 타입:\")\n",
                "print(df_long.dtypes)\n",
                "print(\"\\n샘플 데이터:\")\n",
                "display(df_long.head(3))\n",
                "\n",
                "# 금액: 콤마 제거 후 정수로 변환\n",
                "df_long['amount'] = df_long['amount'].astype(str).str.replace(',', '').astype(int)\n",
                "\n",
                "# 구성비: % 제거 후 실수로 변환\n",
                "df_long['ratio'] = df_long['ratio'].astype(str).str.replace('%', '').astype(float)\n",
                "\n",
                "print(\"\\n변환 후 데이터 타입:\")\n",
                "print(df_long.dtypes)\n",
                "print(\"\\n변환 결과:\")\n",
                "display(df_long.head(5))"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 7: 채널명 영문으로 변경"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 채널명 매핑 딕셔너리\n",
                "channel_map = {\n",
                "    '오프라인': 'offline',\n",
                "    '온라인': 'online',\n",
                "    '기업특판(B2B)': 'b2b',\n",
                "    '기타(로열티)': 'royalty'\n",
                "}\n",
                "\n",
                "# 변경 전\n",
                "print(\"변경 전 채널명:\")\n",
                "print(df_long['channel'].unique())\n",
                "\n",
                "# 영문으로 변경\n",
                "df_long['channel'] = df_long['channel'].map(channel_map)\n",
                "\n",
                "# 변경 후\n",
                "print(\"\\n변경 후 채널명:\")\n",
                "print(df_long['channel'].unique())\n",
                "print(\"\\n변경 결과:\")\n",
                "display(df_long.head(10))"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 8: 정렬"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 연도 → 채널 순으로 정렬\n",
                "df_long = df_long.sort_values(['year', 'channel']).reset_index(drop=True)\n",
                "\n",
                "print(\"정렬 후 최종 결과:\")\n",
                "display(df_long.head(15))\n",
                "print(f\"\\n최종 shape: {df_long.shape}\")\n",
                "print(f\"컬럼: {df_long.columns.tolist()}\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 9: 기본 통계 및 확인"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 결측치 확인\n",
                "print(\"결측치 개수:\")\n",
                "print(df_long.isnull().sum())\n",
                "\n",
                "# 기본 통계\n",
                "print(\"\\n금액 기본 통계:\")\n",
                "display(df_long['amount'].describe())\n",
                "\n",
                "# 연도별 총 금액\n",
                "print(\"\\n연도별 총 금액:\")\n",
                "year_sum = df_long.groupby('year')['amount'].sum().sort_index()\n",
                "display(year_sum)\n",
                "\n",
                "# 채널별 총 금액\n",
                "print(\"\\n채널별 총 금액 (2021-2025 합계):\")\n",
                "channel_sum = df_long.groupby('channel')['amount'].sum().sort_values(ascending=False)\n",
                "display(channel_sum)"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 10: 피벗 테이블로 확인"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 연도/채널별 피벗 테이블\n",
                "print(\"연도/채널별 금액 피벗 테이블:\")\n",
                "pivot_amount = df_long.pivot_table(\n",
                "    values='amount',\n",
                "    index='channel',\n",
                "    columns='year',\n",
                "    aggfunc='sum'\n",
                ")\n",
                "display(pivot_amount)\n",
                "\n",
                "print(\"\\n연도/채널별 구성비 피벗 테이블:\")\n",
                "pivot_ratio = df_long.pivot_table(\n",
                "    values='ratio',\n",
                "    index='channel',\n",
                "    columns='year',\n",
                "    aggfunc='mean'\n",
                ")\n",
                "display(pivot_ratio)"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Step 11: CSV 파일로 저장"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# CSV 파일로 저장\n",
                "df_long.to_csv('판매채널_전처리_완료.csv', index=False, encoding='utf-8-sig')\n",
                "\n",
                "print(\"✅ CSV 저장 완료: 판매채널_전처리_완료.csv\")\n",
                "print(f\"저장된 데이터 수: {len(df_long)}행\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "---\n",
                "\n",
                "## 완료! 전처리 요약\n",
                "\n",
                "**변환 내용:**\n",
                "1. Wide 형태 → Long 형태 변환\n",
                "2. 5개 연도 데이터 통합 (2021-2025)\n",
                "3. 컬럼명 영문 변경 (year, channel, amount, ratio)\n",
                "4. 채널명 영문 변경 (offline, online, b2b, royalty)\n",
                "5. 데이터 타입 변환 (콤마/% 제거)\n",
                "\n",
                "**최종 컬럼:**\n",
                "- year: 연도\n",
                "- channel: 판매 채널\n",
                "- amount: 금액 (원)\n",
                "- ratio: 구성비 (%)\n",
                "\n",
                "**다음 단계:**\n",
                "- 시계열 분석\n",
                "- 채널별 성장률 계산\n",
                "- Tableau/Power BI 시각화"
            ]
        }
    ],
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "codemirror_mode": {
                "name": "ipython",
                "version": 3
            },
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.8.0"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

# JSON 파일로 저장
with open(r'C:\Users\Comet\Desktop\kpdh\code\판매채널_전처리_영문버전.ipynb', 'w', encoding='utf-8') as f:
    json.dump(notebook, f, ensure_ascii=False, indent=1)

print("✅ 주피터 노트북 파일 생성 완료!")
print("파일명: 판매채널_전처리_영문버전.ipynb")
print("\n특징:")
print("- 11개 Step으로 구성")
print("- 각 단계마다 결과 확인 가능")
print("- 피벗 테이블, 통계 포함")
print("- CSV 저장 코드 포함")