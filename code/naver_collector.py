import pandas as pd
import urllib.request
import json
import os
import time
from dotenv import load_dotenv
from functools import reduce

# .env 로드!
load_dotenv()
client_id = os.getenv('NAVER_CLIENT_ID')
client_secret = os.getenv('NAVER_CLIENT_SECRET')

def fetch_naver_datalab(start_date, end_date, keyword_groups, device="", gender="", ages=[]):
    """
    네이버 데이터랩 API 호출 및 DataFrame 반환
    - device: "", "pc", "mo"
    - gender: "", "m", "f"
    - ages: [] (전체) 또는 ["1", "2"] (10대) 등 연령대 코드 리스트
    """
    url = "https://openapi.naver.com/v1/datalab/search"
    
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": "date",
        "keywordGroups": keyword_groups,
        "device": device,
        "ages": ages,
        "gender": gender
    }
    
    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)
    request.add_header("Content-Type", "application/json")
    
    try:
        response = urllib.request.urlopen(request, data=json.dumps(body).encode("utf-8"))
        rescode = response.getcode()
        if rescode == 200:
            result = json.loads(response.read().decode('utf-8'))
            df_list = []
            for i in range(len(result['results'])):
                group_name = result['results'][i]['title']
                data = result['results'][i]['data']
                if data:
                    temp_df = pd.DataFrame(data)
                    temp_df.columns = ['date', group_name]
                    # 메타데이터 추가
                    temp_df['device'] = device if device else 'all'
                    temp_df['gender'] = gender if gender else 'all'
                    temp_df['age_group'] = ",".join(ages) if ages else 'all'
                    df_list.append(temp_df)
            
            if df_list:
                # 합칠 때 메타데이터 컬럼들을 기준점에 포함
                df = reduce(lambda left, right: pd.merge(
                    left, right, on=['date', 'device', 'gender', 'age_group'], how='outer'
                ), df_list)
                return df.fillna(0)
        else:
            print(f"❌ API Error: {rescode}")
    except Exception as e:
        print(f"⚠️ Exception: {e}")
    return None

def collect_demographics(start_date, end_date, groups, title_prefix):
    """다양한 인구통계학적 지표를 순회하며 수집"""
    all_data = []
    
    # 1. 전체 데이터
    print(f"  > [{title_prefix}] 전체 데이터 수집 중...")
    all_data.append(fetch_naver_datalab(start_date, end_date, groups))
    
    # 2. 성별 데이터 (남/여)
    for g in ['m', 'f']:
        print(f"  > [{title_prefix}] 성별({g}) 데이터 수집 중...")
        res = fetch_naver_datalab(start_date, end_date, groups, gender=g)
        if res is not None:
            all_data.append(res)
        time.sleep(0.1)
        
    # 3. 주요 연령대별
    age_bins = {
        "10s": ["1", "2"],
        "20s": ["3", "4"],
        "30s": ["5", "6"],
        "40s_plus": ["7", "8", "9", "10", "11"]
    }
    for label, codes in age_bins.items():
        print(f"  > [{title_prefix}] 연령대({label}) 데이터 수집 중...")
        df = fetch_naver_datalab(start_date, end_date, groups, ages=codes)
        if df is not None:
            df['age_group'] = label
            all_data.append(df)
        time.sleep(0.1)

    valid_dfs = [df for df in all_data if df is not None]
    if valid_dfs:
        return pd.concat(valid_dfs).sort_values(['date', 'gender', 'age_group'])
    return None

# --- 수집 설정 ---

# 주인님의 의견을 반영한 최적화된 키워드 그룹
# (금동대향로, 반가사유상 등 핵심 유물 키워드 추가)
kdh_groups = [
    {
        "groupName": "케데헌_콘텐츠", 
        "keywords": ["케이팝 데몬 헌터스", "케이팝데몬헌터스", "케데헌", "KPop Demon Hunters", "KPDH"]
    },
    {
        "groupName": "국립중앙박물관", 
        "keywords": ["국립중앙박물관", "국중박", "National Museum of Korea", "사유의 방", "사유의방", "금동대향로"]
    },
    {
        "groupName": "뮷즈_굿즈", 
        "keywords": ["뮷즈", "박물관 굿즈", "갓 키링", "박물관 기념품", "케데헌 키링", "반가사유상 미니어처", "반가사유상"]
    }
]

museum_only_groups = [
    {
        "groupName": "국립중앙박물관", 
        "keywords": ["국립중앙박물관", "국중박", "National Museum of Korea", "사유의 방", "사유의방", "금동대향로"]
    },
    {
        "groupName": "뮷즈_굿즈", 
        "keywords": ["뮷즈", "박물관 굿즈", "갓 키링", "박물관 기념품", "반가사유상 미니어처", "반가사유상"]
    }
]

# --- 실행부 ---

# 어제(2026-01-18)까지의 최신 데이터를 수집합니다.
target_end_date = "2026-01-18"

# 1. 수집 5: 케데헌 효과 분석 (2025년~현재 상세)
print(f"🚀 [수집 5] {target_end_date}까지의 케데헌 및 박물관 상세 데이터 수집 시작...")
df5 = collect_demographics("2025-01-01", target_end_date, kdh_groups, "수집5")
if df5 is not None:
    df5.to_csv('naver_kdh_2025_detailed.csv', index=False, encoding='utf-8-sig')
    print("✅ 수집 5 완료: naver_kdh_2025_detailed.csv")

# 2. 수집 6-7: 박물관 7년 장기 추세
print("\n🚀 [수집 6-7] 박물관 7개년 장기 추세 수집 시작...")

def collect_long_term(start, end, prefix):
    results = []
    for g in ["", "m", "f"]:
        print(f"  > [{prefix}] 성별({g if g else 'all'}) 데이터 수집 중...")
        df = fetch_naver_datalab(start, end, museum_only_groups, gender=g)
        if df is not None: results.append(df)
        time.sleep(0.1)
    return pd.concat(results) if results else None

df6 = collect_long_term("2019-01-01", "2023-12-31", "수집6")
df7 = collect_long_term("2021-01-01", target_end_date, "수집7")

# 병합 및 저장
if df6 is not None and df7 is not None:
    df_combined = pd.concat([df6[df6['date'] < '2021-01-01'], df7]).sort_values(['date', 'gender'])
    df_combined.to_csv('naver_museum_7years_demographics.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 7개년 데이터 병합 완료 ({target_end_date}까지): naver_museum_7years_demographics.csv")
