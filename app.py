import streamlit as st
import pandas as pd
import altair as alt
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
SPREADSHEET_ID = "1jwFQ6M-ZHCBoYkGSoT7u8GhNM2ssBZwjfYXvt_FvGGw"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

COURSE_MAP = {
    "코어":     ["코어"],
    "리서치":   ["리서치", "리서치-NLP", "리서치-CV", "리서처"],
    "DS":       ["DS"],
    "PDA":      ["PDA", "프데분"],
    "엔지니어": ["엔지니어"],
}

IRREGULAR_KEYS = ["디스코드 소통왕", "아낌없이 주는 그루", "쉐밸그투", "퍼실재량점수"]

# ─────────────────────────────────────────
# Google Sheets 연결
# ─────────────────────────────────────────
@st.cache_resource
def get_service():
    # 배포 환경: st.secrets 사용 / 로컬: JSON 파일 사용
    try:
        creds = service_account.Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=SCOPES
        )
    except Exception:
        import glob, json
        key_file = glob.glob("*.json")[0]
        creds = service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_sheet(service, sheet_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=sheet_name
    ).execute()
    return result.get("values", [])

def batch_read_sheets(service, sheet_names):
    """여러 시트를 batchGet으로 한 번에 조회 — API 호출 횟수 최소화"""
    if not sheet_names:
        return {}
    result = service.spreadsheets().values().batchGet(
        spreadsheetId=SPREADSHEET_ID,
        ranges=sheet_names,
    ).execute()
    out = {}
    for vr in result.get("valueRanges", []):
        # range 값 예: "'퀘스트(7기코어)'!A1:ZZ999" → 시트명 추출
        raw_range = vr.get("range", "")
        sname = raw_range.split("!")[0].strip("'")
        out[sname] = vr.get("values", [])
    return out

def to_num(val, default=0):
    try:
        return float(str(val).replace(",", "").strip())
    except:
        return default

def quest_val(val, is_main=False):
    v = str(val).strip()
    if is_main:
        # Main QUEST: 숫자 점수 그대로
        try:               return float(v)
        except:            return 0.0
    else:
        # 일반 QUEST: P=1, 숫자(A타입)=그대로, F/null/미제출=0
        if v == "P":       return 1.0
        try:               return float(v)   # QUEST (A) 타입 등 숫자 점수
        except:            return 0.0

def normalize_series(s):
    mn, mx = s.min(), s.max()
    if mx == mn:
        return pd.Series([50.0] * len(s), index=s.index)
    return (s - mn) / (mx - mn) * 100

# ─────────────────────────────────────────
# 데이터 로딩
# ─────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner="스프레드시트 데이터 불러오는 중...")
def load_data():
    service = get_service()
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    all_sheets = [s["properties"]["title"] for s in meta["sheets"]]
    quest_sheets = [s for s in all_sheets if s.startswith("퀘스트")]

    # 노드퀘스트DB → 별점 집계
    db_rows = read_sheet(service, "노드퀘스트DB")
    db_header = db_rows[0]
    col = {h: i for i, h in enumerate(db_header)}

    star_map = {}
    for row in db_rows[1:]:
        if not row or len(row) <= 5:
            continue
        uid    = row[col["고유번호"]] if "고유번호" in col and len(row) > col["고유번호"] else ""
        name   = row[col["이름"]]     if "이름"     in col and len(row) > col["이름"]     else ""
        course = row[col["과정"]]     if "과정"     in col and len(row) > col["과정"]     else ""
        cohort = row[col["기수"]]     if "기수"     in col and len(row) > col["기수"]     else ""
        status = row[col["훈련상태"]] if "훈련상태" in col and len(row) > col["훈련상태"] else ""
        star_raw = row[col["별점"]]   if "별점"     in col and len(row) > col["별점"]     else ""
        try:
            star = float(star_raw)
            star = star if 0 <= star <= 3 else 0
        except:
            star = 0

        # 노드 제출 여부 판단 (별점이 숫자면 제출, 미제출/미참여/null이면 미제출)
        node_name = row[col["노드명"]] if "노드명" in col and len(row) > col["노드명"] else ""
        try:
            float(star_raw)
            node_submitted = True
        except:
            node_submitted = False

        # (고유번호, 이름) 조합을 키로 사용 — 고유번호 중복 할당 시 이름으로 구분
        key = (uid, name) if uid else name
        if key not in star_map:
            star_map[key] = {"이름": name, "과정": course, "기수": cohort,
                             "훈련상태": status, "별점합": 0, "uid": uid,
                             "노드전체": set(), "노드제출": set()}
        star_map[key]["별점합"] += star
        if node_name:
            star_map[key]["노드전체"].add(node_name)
            if node_submitted:
                star_map[key]["노드제출"].add(node_name)

    # 퀘스트 시트 일괄 조회 (batchGet — API 호출 1회)
    records = []
    import re
    all_quest_data = batch_read_sheets(service, quest_sheets)
    for sheet_name in quest_sheets:
        rows = all_quest_data.get(sheet_name, [])
        if len(rows) < 2:
            continue

        h1 = rows[0]
        h2 = rows[1] if len(rows) > 1 else []

        # 역전 헤더 감지: 행0에 '이름'이 없고 행1에 있으면 구조가 역전된 시트
        # 예) 12기리서치 이후: 행0=퀘스트컬럼, 행1=학습자정보컬럼+날짜
        inverted = (not any("이름" in str(x) for x in h1) and
                    any("이름" in str(x) for x in h2))
        if inverted:
            info_header  = h2   # 학습자 정보 컬럼 (이름, 과정, 상태 등)
            quest_header = h1   # 퀘스트 컬럼명
            data_start   = 2    # 데이터는 항상 3행(index 2)부터
        else:
            info_header  = h1
            quest_header = h1
            has_date_row = h2 and any(str(v).startswith("20") for v in h2 if v)
            data_start   = 2 if has_date_row else 1

        def find_col(keyword, header=info_header):
            for i, h in enumerate(header):
                if keyword in str(h):
                    return i
            return None

        uid_col    = find_col("고유번호")
        name_col   = find_col("이름")
        course_col = find_col("과정")
        # status_col: index가 0일 수 있으므로 None 체크
        status_col = find_col("훈련상태")
        if status_col is None:
            status_col = find_col("상태")

        # 비정규 컬럼: 역전 시트는 quest_header(행0)에 있으므로 별도 탐색
        def find_col_in(keyword, header):
            for i, h in enumerate(header):
                if keyword in str(h):
                    return i
            return None
        irr_cols = {k: find_col_in(k, quest_header) for k in IRREGULAR_KEYS
                    if find_col_in(k, quest_header) is not None}

        def is_main_quest(h):
            h = str(h)
            return "Main QUEST" in h or "[PROJECT] Main" in h

        def is_regular_quest(h):
            h = str(h)
            if is_main_quest(h):
                return False
            return ("QUEST" in h.upper() or "Sub Quest" in h or "QuestB" in h)

        quest_cols      = [i for i, h in enumerate(quest_header) if is_regular_quest(h)]
        main_quest_cols = [i for i, h in enumerate(quest_header) if is_main_quest(h)]

        for row in rows[data_start:]:
            if not row:
                continue
            uid    = row[uid_col].strip()    if uid_col    is not None and len(row) > uid_col    else ""
            name   = row[name_col].strip()   if name_col   is not None and len(row) > name_col   else ""
            course = row[course_col].strip() if course_col is not None and len(row) > course_col else ""
            status = row[status_col].strip() if status_col is not None and len(row) > status_col else ""

            if not name:
                continue

            q_score  = sum(quest_val(row[i], is_main=False) for i in quest_cols      if i < len(row))
            mq_score = sum(quest_val(row[i], is_main=True)  for i in main_quest_cols  if i < len(row))

            irr_detail = {k: to_num(row[v]) if v < len(row) else 0 for k, v in irr_cols.items()}
            irr_total  = sum(irr_detail.values())

            lookup_key = (uid, name) if uid else name
            star_info  = star_map.get(lookup_key, {})
            node_star  = star_info.get("별점합", 0)

            # 퀘스트 제출율 (P/F/숫자 = 제출, null/미제출/미참여/빈값 = 미제출)
            NOT_SUBMITTED = {"null", "미제출", "미참여", "중도포기", ""}
            all_quest_cols = quest_cols + main_quest_cols
            total_quests = len(all_quest_cols)
            submitted_quests = sum(
                1 for i in all_quest_cols
                if i < len(row) and str(row[i]).strip() not in NOT_SUBMITTED
            )
            quest_submit_rate = (submitted_quests / total_quests * 100) if total_quests > 0 else 0

            # 노드 제출율 (star_map의 노드 집합 기반)
            node_total = len(star_info.get("노드전체", set()))
            node_submitted_cnt = len(star_info.get("노드제출", set()))
            node_submit_rate = (node_submitted_cnt / node_total * 100) if node_total > 0 else 0

            # 성실점수 = 퀘스트 제출율과 노드 제출율의 평균
            sincerity = (quest_submit_rate + node_submit_rate) / 2

            if not course:
                if "코어" in sheet_name:        course = "코어"
                elif "리서치" in sheet_name:    course = "리서치"
                elif "데싸" in sheet_name:      course = "DS"
                elif "프데분" in sheet_name:    course = "PDA"
                elif "엔지니어" in sheet_name:  course = "엔지니어"
                else:                           course = "기타"

            # 기수는 항상 시트명에서 추출 (노드퀘스트DB의 기수는 "6기" 등 prefix 없이 달라 충돌)
            m = re.search(r"(데싸\d+기|프데분\d+기|엔지니어\d+기|쏘카|\d+기코어|\d+기리서치|\d+기)", sheet_name)
            cohort = m.group(1) if m else sheet_name
            # 코어/리서치 suffix 제거해서 기수만 남김
            cohort = re.sub(r"(코어|리서치)$", "", cohort)

            records.append({
                "고유번호": uid, "이름": name, "과정": course,
                "기수": cohort, "훈련상태": status,
                "퀘스트점수": q_score, "메인퀘스트점수": mq_score,
                "퀘스트합계": q_score + mq_score,
                "노드별점": node_star, "비정규합계": irr_total,
                "퀘스트제출율": round(quest_submit_rate, 1),
                "노드제출율": round(node_submit_rate, 1),
                "성실점수": round(sincerity, 1),
                **{f"[비정규]{k}": v for k, v in irr_detail.items()},
            })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    def normalize_course(c):
        for label, variants in COURSE_MAP.items():
            for v in variants:
                if v in str(c):
                    return label
        return "기타"

    df["과정분류"] = df["과정"].apply(normalize_course)

    # 기수 정렬 키
    def cohort_sort_key(c):
        m = re.search(r"(\d+)", str(c))
        prefix = re.sub(r"\d", "", str(c))
        return (prefix, int(m.group(1)) if m else 0)

    df["_기수정렬"] = df["기수"].apply(cohort_sort_key)
    return df


# ─────────────────────────────────────────
# 기수별 만점 테이블 (과정분류, 기수) → (노드만점, 퀘스트만점)
# 미등록 기수는 그룹 내 Min-Max로 fallback
# ─────────────────────────────────────────
MAX_SCORES = {
    ("코어",   "5기"):      (39, 94),
    ("코어",   "6기"):      (39, 84),
    ("코어",   "7기"):      (39, 90),
    ("코어",   "8기"):      (39, 93),
    ("코어",   "9기"):      (39, 93),
    ("코어",   "10기"):     (39, 93),
    ("코어",   "11기"):     (39, 93),
    ("코어",   "12기"):     (39, 93),
    ("리서치", "5기"):      (57, 96),
    ("리서치", "6기"):      (57, 96),
    ("리서치", "7기"):      (57, 98),
    ("리서치", "8기"):      (48, 86),
    ("리서치", "9기"):      (48, 84),
    ("리서치", "10기"):     (48, 84),
    ("리서치", "12기"):     (48, 84),
    ("리서치", "13기"):     (48, 93),
    ("리서치", "14기"):     (48, 89),
    ("리서치", "15기"):     (48, 89),
    ("리서치", "16기"):     (54, 89),
    ("엔지니어", "1기"):    (24, 85),
    ("DS",     "데싸1기"):  (69, 89),
    ("DS",     "데싸2기"):  (45, 89),
    ("DS",     "데싸3기"):  (36, 88),
    ("DS",     "데싸4기"):  (36, 85),
    ("DS",     "데싸5기"):  (39, 90),
    ("DS",     "데싸6기"):  (36, 91),
    ("DS",     "데싸7기"):  (36, 91),
    ("DS",     "데싸8기"):  (36, 91),
    ("PDA",    "프데분1기"): (33, 75),
    ("PDA",    "프데분2기"): (21, 75),
    ("PDA",    "프데분3기"): (15, 75),
    ("PDA",    "프데분4기"): (18, 75),
    ("PDA",    "프데분5기"): (15, 74),
}

def normalize_by_max(series, max_val):
    """만점 기준 정규화 (0~100, 초과분은 100으로 클리핑)"""
    if max_val <= 0:
        return pd.Series([0.0] * len(series), index=series.index)
    return (series / max_val * 100).clip(upper=100.0)

# ─────────────────────────────────────────
# 점수 계산
# ─────────────────────────────────────────
def compute_scores(df, w_quest, w_star, w_irr, w_sincerity):
    total_w = w_quest + w_star + w_irr + w_sincerity
    if total_w == 0:
        return df

    result = []
    for (과정, 기수), grp in df.groupby(["과정분류", "기수"]):
        grp = grp.copy()
        max_info = MAX_SCORES.get((과정, 기수))

        if max_info:
            node_max, quest_max = max_info
            grp["퀘스트_정규화"] = normalize_by_max(grp["퀘스트합계"], quest_max)
            grp["별점_정규화"]   = normalize_by_max(grp["노드별점"],   node_max)
        else:
            grp["퀘스트_정규화"] = normalize_series(grp["퀘스트합계"])
            grp["별점_정규화"]   = normalize_series(grp["노드별점"])

        # 비정규: IQR 적응형 윈저라이징 후 Min-Max (이상치 없으면 clip 무효)
        q3  = grp["비정규합계"].quantile(0.75)
        iqr = q3 - grp["비정규합계"].quantile(0.25)
        irr_cap = q3 + 1.5 * iqr
        grp["비정규_정규화"] = normalize_series(grp["비정규합계"].clip(upper=irr_cap))
        # 성실점수: 이미 0~100 비율이므로 정규화 없이 그대로 사용
        grp["성실_정규화"] = grp["성실점수"].clip(0, 100)

        grp["종합점수"] = (
            grp["퀘스트_정규화"] * w_quest     +
            grp["별점_정규화"]   * w_star       +
            grp["비정규_정규화"] * w_irr        +
            grp["성실_정규화"]   * w_sincerity
        ) / total_w
        result.append(grp)

    return pd.concat(result).reset_index(drop=True)


# ─────────────────────────────────────────
# UI
# ─────────────────────────────────────────
st.set_page_config(page_title="우수수료생 선정 도구", layout="wide")
st.title("우수수료생 선정 도구")
st.caption("과정·기수별로 점수 기준을 넘은 학습자를 우수수료생으로 선정합니다.")

df_all = load_data()
if df_all.empty:
    st.error("데이터를 불러오지 못했습니다.")
    st.stop()

# ── 사이드바 ──────────────────────────────
st.sidebar.header("점수 반영 비율")
w_quest     = st.sidebar.slider("퀘스트 점수 (QUEST + Main QUEST)", 0, 100, 50, step=5)
w_star      = st.sidebar.slider("노드 별점", 0, 100, 20, step=5)
w_irr       = st.sidebar.slider("비정규 점수", 0, 100, 20, step=5)
w_sincerity = st.sidebar.slider("성실 점수 (퀘스트+노드 제출율 평균)", 0, 100, 10, step=5)

total_w = w_quest + w_star + w_irr + w_sincerity
if total_w == 0:
    st.sidebar.error("비율 합계가 0입니다.")
    st.stop()
st.sidebar.caption(f"합계: {total_w}")

st.sidebar.divider()
st.sidebar.header("필터")

course_options = ["전체"] + [k for k in COURSE_MAP if k in df_all["과정분류"].unique()]
selected_course = st.sidebar.selectbox("과정", course_options)

status_options = sorted(df_all["훈련상태"].unique())
default_status = [s for s in status_options if s in ["정상수료", "훈련중", "80%이상수료", "조기취업", "수료후취업"]]
selected_status = st.sidebar.multiselect("훈련상태", status_options,
                                         default=default_status if default_status else status_options)

# ── 데이터 필터 & 점수 계산 ───────────────
df = df_all.copy()
if selected_course != "전체":
    df = df[df["과정분류"] == selected_course]
if selected_status:
    df = df[df["훈련상태"].isin(selected_status)]

df = compute_scores(df, w_quest, w_star, w_irr, w_sincerity)

st.sidebar.divider()
st.sidebar.header("우수수료생 기준")
cutoff_score = st.sidebar.slider("기준 점수 (0~100)", 0.0, 100.0, 80.0, step=1.0)
st.sidebar.caption("과정+기수 내 Min-Max 정규화(0~100) 후 가중합산한 종합점수 기준")

df["우수수료생"] = df["종합점수"] >= cutoff_score

# ── 요약 메트릭 ──────────────────────────
total_students = len(df)
total_excellent = df["우수수료생"].sum()
group_count = df.groupby(["과정분류", "기수"]).ngroups

c1, c2, c3, c4 = st.columns(4)
c1.metric("전체 대상자", f"{total_students}명")
c2.metric("우수수료생 선정", f"{int(total_excellent)}명")
c3.metric("선정률", f"{total_excellent/total_students*100:.1f}%" if total_students else "0%")
c4.metric("과정+기수 그룹 수", f"{group_count}개")

st.divider()

# ── 탭 구성 ──────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["기수별 우수수료생", "전체 순위표", "과정별 요약", "점수 분포"])

DISP = ["이름", "훈련상태", "퀘스트합계", "노드별점", "비정규합계", "종합점수", "우수수료생"]
irr_detail_cols = [c for c in df.columns if c.startswith("[비정규]")]

# ────────────────────────────────────────
# TAB 1 : 기수별 우수수료생
# ────────────────────────────────────────
with tab1:
    # 과정분류 → 기수 순으로 그룹
    course_order = [k for k in COURSE_MAP if k in df["과정분류"].unique()]
    if selected_course != "전체":
        course_order = [selected_course]

    for course_label in course_order:
        df_course = df[df["과정분류"] == course_label]
        if df_course.empty:
            continue

        st.header(f"📌 {course_label}")

        cohorts = sorted(df_course["기수"].unique(),
                         key=lambda c: df_course[df_course["기수"] == c]["_기수정렬"].iloc[0])

        for cohort in cohorts:
            grp = df_course[df_course["기수"] == cohort].sort_values("종합점수", ascending=False)
            excellent = grp[grp["우수수료생"]]
            n_total = len(grp)
            n_exc   = len(excellent)

            label = f"**{cohort}**  —  전체 {n_total}명 중 우수수료생 {n_exc}명"
            badge = "🏆" if n_exc > 0 else "—"

            with st.expander(f"{badge} {label}", expanded=(n_exc > 0)):
                col_a, col_b = st.columns([1, 2])

                with col_a:
                    st.metric("우수수료생 / 전체", f"{n_exc} / {n_total}")
                    st.metric("선정률", f"{n_exc/n_total*100:.1f}%" if n_total else "0%")
                    st.metric("기준 점수", f"{cutoff_score:.1f}")

                with col_b:
                    # 막대 차트 (종합점수) — 추가 추천 포함
                    min_15pct_chart = max(1, int(n_total * 0.15))
                    top15_names = set(grp.head(min_15pct_chart)["이름"]) if n_exc < min_15pct_chart else set()
                    chart_df = grp[["이름", "종합점수", "우수수료생"]].copy()
                    def _label(row):
                        if row["우수수료생"]:
                            return "우수수료생"
                        if row["이름"] in top15_names:
                            return "추가 추천"
                        return "일반"
                    chart_df["구분"] = chart_df.apply(_label, axis=1)
                    bar = alt.Chart(chart_df).mark_bar().encode(
                        x=alt.X("종합점수:Q", scale=alt.Scale(domain=[0, 100])),
                        y=alt.Y("이름:N", sort="-x"),
                        color=alt.Color("구분:N",
                            scale=alt.Scale(domain=["우수수료생", "추가 추천", "일반"],
                                            range=["#2ecc71", "#f0c040", "#bdc3c7"])),
                        tooltip=["이름", "종합점수", "구분"]
                    ).properties(height=max(80, n_total * 25))
                    cutline = alt.Chart(pd.DataFrame({"x": [cutoff_score]})).mark_rule(
                        color="red", strokeDash=[4, 3], strokeWidth=1.5
                    ).encode(x="x:Q")
                    st.altair_chart(bar + cutline, use_container_width=True)

                if n_exc > 0:
                    st.markdown("**우수수료생 명단**")

                    # 원점수 + 정규화 점수 함께 표시
                    disp = excellent[["이름", "훈련상태",
                                      "퀘스트합계",  "퀘스트_정규화",
                                      "노드별점",    "별점_정규화",
                                      "비정규합계",  "비정규_정규화",
                                      "퀘스트제출율", "노드제출율", "성실_정규화",
                                      "종합점수"]].reset_index(drop=True)
                    disp.columns = ["이름", "훈련상태",
                                    "퀘스트(원점수)", "퀘스트(정규화)",
                                    "별점(원점수)",   "별점(정규화)",
                                    "비정규(원점수)", "비정규(정규화)",
                                    "퀘스트제출율(%)", "노드제출율(%)", "성실(정규화)",
                                    "종합점수"]
                    italic_cols = ["퀘스트(정규화)", "별점(정규화)", "비정규(정규화)", "성실(정규화)"]
                    st.dataframe(
                        disp.style.format({
                            "퀘스트(원점수)":  "{:.1f}", "퀘스트(정규화)":  "{:.1f}",
                            "별점(원점수)":    "{:.1f}", "별점(정규화)":    "{:.1f}",
                            "비정규(원점수)":  "{:.1f}", "비정규(정규화)":  "{:.1f}",
                            "퀘스트제출율(%)": "{:.1f}", "노드제출율(%)":   "{:.1f}",
                            "성실(정규화)":    "{:.1f}", "종합점수":        "{:.2f}",
                        }).map(lambda _: "background-color:#d4edda", subset=["종합점수"])
                         .map(lambda _: "color:#666666; font-style:italic", subset=italic_cols),
                        use_container_width=True,
                    )

                    if irr_detail_cols:
                        with st.expander("비정규 점수 상세"):
                            st.dataframe(
                                excellent[["이름"] + irr_detail_cols]
                                    .reset_index(drop=True)
                                    .style.format({c: "{:.1f}" for c in irr_detail_cols}),
                                use_container_width=True,
                            )
                else:
                    st.info("기준 점수를 넘은 학습자가 없습니다.")

                # ── 상위 15% 추가 추천 ──
                min_15pct = max(1, int(n_total * 0.15))
                if n_total > 0 and n_exc < min_15pct:
                    top10 = grp.head(min_15pct)
                    extra = top10[~top10["우수수료생"]]
                    if not extra.empty:
                        st.markdown(
                            f"⚠️ **추가 추천**: 우수수료생이 전체의 "
                            f"{n_exc/n_total*100:.1f}%로 15% 미만입니다. "
                            f"종합점수 상위 15% ({min_15pct}명) 중 미선정 "
                            f"{len(extra)}명을 추천합니다."
                        )
                        extra_disp = extra[["이름", "훈련상태",
                                            "퀘스트합계",  "퀘스트_정규화",
                                            "노드별점",    "별점_정규화",
                                            "비정규합계",  "비정규_정규화",
                                            "퀘스트제출율", "노드제출율", "성실_정규화",
                                            "종합점수"]].reset_index(drop=True)
                        extra_disp.columns = ["이름", "훈련상태",
                                              "퀘스트(원점수)", "퀘스트(정규화)",
                                              "별점(원점수)",   "별점(정규화)",
                                              "비정규(원점수)", "비정규(정규화)",
                                              "퀘스트제출율(%)", "노드제출율(%)", "성실(정규화)",
                                              "종합점수"]
                        italic_cols2 = ["퀘스트(정규화)", "별점(정규화)", "비정규(정규화)", "성실(정규화)"]
                        st.dataframe(
                            extra_disp.style.format({
                                "퀘스트(원점수)":  "{:.1f}", "퀘스트(정규화)":  "{:.1f}",
                                "별점(원점수)":    "{:.1f}", "별점(정규화)":    "{:.1f}",
                                "비정규(원점수)":  "{:.1f}", "비정규(정규화)":  "{:.1f}",
                                "퀘스트제출율(%)": "{:.1f}", "노드제출율(%)":   "{:.1f}",
                                "성실(정규화)":    "{:.1f}", "종합점수":        "{:.2f}",
                            }).map(lambda _: "background-color:#fff3cd", subset=["종합점수"])
                             .map(lambda _: "color:#666666; font-style:italic", subset=italic_cols2),
                            use_container_width=True,
                        )

# ────────────────────────────────────────
# TAB 2 : 전체 순위표
# ────────────────────────────────────────
with tab2:
    st.subheader("전체 학습자 순위 (과정+기수 내 정규화 점수 기준)")

    show_df = df.sort_values(["과정분류", "_기수정렬", "종합점수"],
                             ascending=[True, True, False]).copy()
    show_df["순위"] = show_df.groupby(["과정분류", "기수"])["종합점수"] \
                             .rank(ascending=False, method="min").astype(int)

    disp = show_df[["과정분류", "기수", "순위", "이름", "훈련상태",
                     "퀘스트합계",  "퀘스트_정규화",
                     "노드별점",    "별점_정규화",
                     "비정규합계",  "비정규_정규화",
                     "퀘스트제출율", "노드제출율", "성실_정규화",
                     "종합점수", "우수수료생"]].reset_index(drop=True)
    disp.columns = ["과정분류", "기수", "순위", "이름", "훈련상태",
                    "퀘스트(원점수)", "퀘스트(정규화)",
                    "별점(원점수)",   "별점(정규화)",
                    "비정규(원점수)", "비정규(정규화)",
                    "퀘스트제출율(%)", "노드제출율(%)", "성실(정규화)",
                    "종합점수", "우수수료생"]

    def highlight_row(row):
        if row["우수수료생"]:
            return ["background-color:#d4edda"] * len(row)
        return [""] * len(row)

    italic_cols2 = ["퀘스트(정규화)", "별점(정규화)", "비정규(정규화)", "성실(정규화)"]
    st.dataframe(
        disp.style.apply(highlight_row, axis=1)
            .format({
                "퀘스트(원점수)":  "{:.1f}", "퀘스트(정규화)":  "{:.1f}",
                "별점(원점수)":    "{:.1f}", "별점(정규화)":    "{:.1f}",
                "비정규(원점수)":  "{:.1f}", "비정규(정규화)":  "{:.1f}",
                "퀘스트제출율(%)": "{:.1f}", "노드제출율(%)":   "{:.1f}",
                "성실(정규화)":    "{:.1f}", "종합점수":        "{:.2f}",
            }).map(lambda _: "color:#666666; font-style:italic", subset=italic_cols2),
        use_container_width=True,
        height=600,
    )

# ────────────────────────────────────────
# TAB 3 : 과정별 요약
# ────────────────────────────────────────
with tab3:
    st.subheader("과정별 우수수료생 현황")

    # 과정별 집계
    summary = df.groupby("과정분류").agg(
        전체=("이름", "count"),
        우수수료생=("우수수료생", "sum"),
        퀘스트평균=("퀘스트합계", "mean"),
        퀘스트중앙=("퀘스트합계", "median"),
        별점평균=("노드별점", "mean"),
        별점중앙=("노드별점", "median"),
        비정규평균=("비정규합계", "mean"),
        비정규중앙=("비정규합계", "median"),
    ).reset_index()
    summary["선정률(%)"] = (summary["우수수료생"] / summary["전체"] * 100).round(1)
    summary["우수수료생"] = summary["우수수료생"].astype(int)

    # 과정별 우수수료생 수 막대
    bar_exc = alt.Chart(summary).mark_bar().encode(
        x=alt.X("과정분류:N", sort="-y", title="과정"),
        y=alt.Y("우수수료생:Q", title="우수수료생 수"),
        color=alt.Color("과정분류:N", legend=None),
        tooltip=["과정분류", "전체", "우수수료생", "선정률(%)"]
    ).properties(height=250, title="과정별 우수수료생 수")

    bar_rate = alt.Chart(summary).mark_bar().encode(
        x=alt.X("과정분류:N", sort="-y", title="과정"),
        y=alt.Y("선정률(%):Q", title="선정률 (%)", scale=alt.Scale(domain=[0, 100])),
        color=alt.Color("과정분류:N", legend=None),
        tooltip=["과정분류", "전체", "우수수료생", "선정률(%)"]
    ).properties(height=250, title="과정별 선정률 (%)")

    st.altair_chart(alt.hconcat(bar_exc, bar_rate), use_container_width=True)

    # 요약 테이블
    st.dataframe(
        summary[["과정분류", "전체", "우수수료생", "선정률(%)",
                 "퀘스트평균", "퀘스트중앙", "별점평균", "별점중앙", "비정규평균", "비정규중앙"]]
            .style.format({
                "선정률(%)": "{:.1f}%",
                "퀘스트평균": "{:.2f}", "퀘스트중앙": "{:.2f}",
                "별점평균": "{:.2f}",   "별점중앙": "{:.2f}",
                "비정규평균": "{:.2f}", "비정규중앙": "{:.2f}",
            }),
        use_container_width=True,
    )

    st.divider()
    st.subheader("과정별 점수 분포 (Box Plot)")

    # 퀘스트 / 별점 / 비정규를 long format으로 변환
    score_long = df[["이름", "과정분류", "퀘스트합계", "노드별점", "비정규합계"]].melt(
        id_vars=["이름", "과정분류"],
        value_vars=["퀘스트합계", "노드별점", "비정규합계"],
        var_name="점수항목", value_name="점수"
    )
    score_long["점수항목"] = score_long["점수항목"].map({
        "퀘스트합계": "퀘스트 점수",
        "노드별점":   "노드 별점",
        "비정규합계": "비정규 점수",
    })

    box = alt.Chart(score_long).mark_boxplot(extent="min-max").encode(
        x=alt.X("과정분류:N", title="과정"),
        y=alt.Y("점수:Q", title="점수"),
        color=alt.Color("과정분류:N", legend=None),
        column=alt.Column("점수항목:N", title=""),
        tooltip=["과정분류", "점수항목", "점수"]
    ).properties(height=280, width=160)

    st.altair_chart(box)
    st.caption("박스플롯: 중앙선=중앙값, 박스=Q1~Q3, 수염=전체 범위")

    st.divider()
    st.subheader("과정별 평균 점수 비교 (레이더 대용 막대)")

    # 정규화된 평균 비교
    norm_summary = df.groupby("과정분류").agg(
        퀘스트=("퀘스트_정규화", "mean"),
        별점=("별점_정규화", "mean"),
        비정규=("비정규_정규화", "mean"),
    ).reset_index()

    norm_long = norm_summary.melt(
        id_vars="과정분류",
        value_vars=["퀘스트", "별점", "비정규"],
        var_name="항목", value_name="평균(정규화)"
    )

    grouped_bar = alt.Chart(norm_long).mark_bar().encode(
        x=alt.X("항목:N", title=""),
        y=alt.Y("평균(정규화):Q", title="평균 점수 (0~100 정규화)", scale=alt.Scale(domain=[0, 100])),
        color=alt.Color("항목:N", legend=alt.Legend(title="항목")),
        column=alt.Column("과정분류:N", title="과정"),
        tooltip=["과정분류", "항목", alt.Tooltip("평균(정규화):Q", format=".1f")]
    ).properties(height=250, width=120)

    st.altair_chart(grouped_bar)
    st.caption("각 과정 내 정규화(0~100) 기준 항목별 평균 점수 비교")


# ────────────────────────────────────────
# TAB 4 : 점수 분포
# ────────────────────────────────────────
with tab4:
    st.subheader("종합점수 분포")

    hist_df = df[["이름", "과정분류", "기수", "종합점수", "우수수료생"]].copy()
    hist_df["구분"] = hist_df["우수수료생"].map({True: "우수수료생", False: "일반"})

    hist = alt.Chart(hist_df).mark_bar(opacity=0.8).encode(
        x=alt.X("종합점수:Q", bin=alt.Bin(maxbins=30), title="종합점수"),
        y=alt.Y("count():Q", title="인원수"),
        color=alt.Color("구분:N",
            scale=alt.Scale(domain=["우수수료생", "일반"], range=["#2ecc71", "#bdc3c7"])),
        tooltip=["구분", "count()"]
    ).properties(height=300)

    cutline = alt.Chart(pd.DataFrame({"x": [cutoff_score]})).mark_rule(
        color="red", strokeDash=[5, 3], strokeWidth=2
    ).encode(x="x:Q")

    st.altair_chart(hist + cutline, use_container_width=True)
    st.caption("빨간 점선 = 기준 점수")

    st.subheader("퀘스트 vs 별점 산점도")
    scatter_df = df[["이름", "과정분류", "기수", "종합점수",
                     "퀘스트_정규화", "별점_정규화", "비정규_정규화", "우수수료생"]].copy()
    scatter_df["구분"] = scatter_df["우수수료생"].map({True: "우수수료생", False: "일반"})

    scatter = alt.Chart(scatter_df).mark_circle(size=90).encode(
        x=alt.X("퀘스트_정규화:Q", title="퀘스트 점수 (정규화)"),
        y=alt.Y("별점_정규화:Q",   title="노드 별점 (정규화)"),
        color=alt.Color("구분:N",
            scale=alt.Scale(domain=["우수수료생", "일반"], range=["#2ecc71", "#bdc3c7"])),
        size=alt.Size("비정규_정규화:Q", legend=alt.Legend(title="비정규(크기)")),
        tooltip=["이름", "기수", "과정분류", "종합점수"]
    ).properties(height=400)

    st.altair_chart(scatter, use_container_width=True)
