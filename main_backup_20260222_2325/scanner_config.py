DISCORD_WEBHOOK_URL = "weebhook"  # ⚠️ 반드시 새로 만든 웹훅 사용 권장(노출되면 스팸 가능)

ACCOUNT_EQUITY_USD = 83000   # 계좌 총액(달러)
RISK_PER_TRADE = 0.005        # 0.5% (1R 손실을 계좌의 0.5%로)

MAX_BUY_PER_DAY = 10
MIN_RR = 1.8

LOOKBACK_DAYS = 2000
MIN_DOLLAR_VOL = 15_000_000

# 스윙용 신호 파라미터
BREAKOUT_NEAR = 0.985
BREAKOUT_VOL_X = 1.25
PULLBACK_BAND = 0.015
ATR_STOP_MULT = 2.0

# 추세 강도 필터 (ADX): 이 값 미만이면 BUY → WATCH_ADX로 내림
MIN_ADX = 20   # 20 미만 = 추세 없음, 25 이상 = 추세 있음

# Relative Strength (시장 대비 강도): SPY보다 약하면 BUY → WATCH_RS
RS_LOOKBACK_DAYS = 20   # 이 기간 수익률로 종목 vs SPY 비교

# =========================
# 미네르비니 스타일 필터 (선택 적용, 주의점 있음)
# =========================
# 1) 52주 고점 근접: 고점에서 너무 멀면(저가 근처) WATCH_52WK. 브레이크아웃 직후는 걸릴 수 있으니 WATCH로 완화.
MINERVINI_52WK_ENABLED = True
MINERVINI_52WK_MAX_PCT_OFF = 0.25   # 52주 고점 대비 이 비율보다 멀면(예: 25% 아래) WATCH_52WK
MINERVINI_52WK_MAX_PCT_OFF_PULLBACK = 0.35   # 눌림매수(BUY_PULLBACK)만 완화: 35% 이내 허용 (풀백은 고점에서 멀 수 있음)

# 2) 이평 정렬(50 > 150 > 200) + 가격이 모두 위: 켜면 미네르비니 추세 구조 강화. 끄면 기존만.
MINERVINI_MA_STACK_ENABLED = True

# 3) 펀더멘털(실적): 단일종목(ADD_BUY/검색)에서만 적용. 배치 스캔은 API 부하로 미적용.
MINERVINI_FUNDAMENTAL_ENABLED = False
MINERVINI_EPS_GROWTH_MIN_PCT = 20   # 분기 EPS 성장률 % (yfinance는 소수면 0.20 = 20%)
MINERVINI_REVENUE_GROWTH_MIN_PCT = 15
MINERVINI_ROE_MIN_PCT = 17

# =========================
# 2단계: 가짜 돌파 필터(진짜 돌파만 BUY)
# =========================
BREAKOUT_CLOSE_BUFFER = 0.001   # 돌파는 '고가 터치'가 아니라 '종가가 고점 위'로 마감해야 함(0.1%)
BREAKOUT_MAX_WICK_RATIO = 0.60  # 윗꼬리 비율이 너무 크면(매도압력) 돌파 실패로 간주
BREAKOUT_MAX_GAP_ATR = 1.20     # 전일 대비 갭이 ATR의 1.2배 넘으면 추격매수 위험 → WATCH로 내림


TARGET_LOOKBACK_1 = 20
TARGET_LOOKBACK_2 = 60
TARGET_ATR_FLOOR_BREAKOUT = 3.0   # 돌파는 최소 +3ATR 위
TARGET_ATR_FLOOR_PULLBACK = 2.5   # 눌림은 최소 +2.5ATR 위
# 시총 필터(50~100조원 구간)
USE_MKT_CAP_FILTER = False
KRW_PER_USD = 1464.0
MKT_CAP_MIN_KRW = 50_000_000_000_000
MKT_CAP_MAX_KRW = 100_000_000_000_000

# 섹터 분산
USE_SECTOR_CAP = True
MAX_PER_SECTOR = 3

# WATCH 알림 최대 몇개
WATCH_LIMIT = 30

# 메타 캐시(시총/섹터) 파일
META_CACHE_PATH = "meta_cache.csv"

# 최초 실행 때 meta_cache.csv가 없으면 info로 채우는데, 느릴 수 있음
# 당장 빠르게 돌리고 싶으면 False로 두면 "Unknown/None"으로 진행
ALLOW_META_FETCH_IF_MISSING = True

# 데이터 최신성 체크
MAX_DATA_STALE_DAYS = 5

# 돌파 BUY 거래량 기준(기존 BREAKOUT_VOL_X보다 더 엄격하게)
BUY_BREAKOUT_VOL_X = 1.5

# BUY 시 MACD 확인을 필수로 할지 (원하면 True)
REQUIRE_MACD_CONFIRM_FOR_BUY = False

SKIP_PRINT_LIMIT = 50

# ✅ SMART RELAX: BUY가 너무 적을 때만 조건 완화
SMART_RELAX_ENABLED = True
SMART_MIN_BUY = 3              # BUY가 3개 미만이면 완화 작동
SMART_MIN_RR = 1.5             # 완화된 최소 RR
SMART_BUY_BREAKOUT_VOL_X = 1.25 # 완화된 돌파 거래량 기준(VolRatio >= 이 값)


# 눌림매수 품질 필터 (가짜 반등 제거)
USE_PULLBACK_QUALITY_FILTER = True

# =========================
# ATR 기반 부분익절 목표가 (최적 세팅)
# - ATR_STOP_MULT=2.0 (현재 코드) 기준으로 설계
# =========================

# HOLD일 때(기본): 1차=리스크 제거, 2차=추세, 3차=런(run)
TP_ATR_M1 = 2.0   # ≈ +1R 근처 (리스크 제거/멘탈 안정)
TP_ATR_M2 = 4.0   # ≈ +2R (추세 구간)
TP_ATR_M3 = 6.0   # ≈ +3R (강한 추세 런)

# ADD_BUY일 때(상향): 조기익절 방지용으로 2차/3차 더 멀게
TP_ATR_BOOST_M1 = 2.5
TP_ATR_BOOST_M2 = 5.5
TP_ATR_BOOST_M3 = 8.0

# =========================
# 목표가 캡: High60(최근 60일 고점) 기반
# =========================
TP_USE_HIGH60_CAP = True

# 2차/3차 목표가가 High60 대비 너무 멀어지는 걸 방지
# 예: High60 * 1.03 이내로 캡이면 "High60 근처까지만" 목표가 설정
TP_CAP_H60_MULT_2 = 1.02   # 2차 목표가 상한 = High60 * 1.02
TP_CAP_H60_MULT_3 = 1.05   # 3차 목표가 상한 = High60 * 1.05

# 반대로 너무 낮게 잡히는 걸 방지(최소 보장)
TP_FLOOR_ATR_M2 = 3.0      # 2차는 최소 close + ATR*3
TP_FLOOR_ATR_M3 = 4.5      # 3차는 최소 close + ATR*4.5

# =========================
# High60 돌파 이후 캡 자동 해제
# =========================
TP_CAP_DISABLE_ON_BREAKOUT = True
TP_CAP_DISABLE_BUFFER = 0.002   # 0.2% 위에서 종가 마감이면 "돌파 확인"으로 간주



# =========================
# SELL 보수화 옵션 (Holding Risk Review)
# =========================
SELL_CONFIRM_ENABLED = True
SELL_CONFIRM_MODE = "any"   # "any" or "all"

SELL_CONFIRM_USE_MACD = True
SELL_CONFIRM_USE_VOL  = True
SELL_CONFIRM_VOL_X = 1.5
SELL_CONFIRM_USE_2D   = True

# 구조 붕괴(스윙 로우 이탈) 매도: 최근 N봉 스윙 로우 아래로 종가 이탈 시 추가 매도 조건
SELL_SWING_LOW_LOOKBACK = 20   # 최근 N봉(오늘 제외) 중 최저가 = 스윙 로우, 종가 < 스윙 로우 → SELL_STRUCTURE_BREAK

# 분배(Distribution) 경고: 참고용만, 실제 청산은 트레일/구조붕괴 등으로만 수행
# 최근 N일 중 "하락일 + 거래량이 20일평균 이상"인 날이 M일 이상이면 분배 경고
DISTRIBUTION_LOOKBACK = 10
DISTRIBUTION_MIN_DAYS = 4   # 이 일수 이상이면 ⚠ 분배 경고를 Reason에 추가

# 추세약화(Slope) 경고: SMA20 5봉 기울기 음전 + 상승률 감소 시 참고용 경고
SLOPE_SMA20_LOOKBACK = 5   # 5봉 고정으로 기울기 계산

# 변동성 붕괴(ATR Compression) 경고: 참고용. ATR%가 N일 전 대비 X% 이상 감소 시
ATR_COMPRESSION_LOOKBACK = 10   # N일 전 ATR%와 비교
ATR_COMPRESSION_PCT_DROP = 0.20   # 20% 이상 감소 시 ⚠ 변동성 붕괴 경고

# 손실 확대 경고: 진입가 대비 손실이 N% 이상이면 Reason에 참고용 경고 (실제 청산은 트레일/구조붕괴 등)
LOSS_WARNING_PCT = 7   # 손실 7% 이상이면 ⚠ 손실 확대 경고

# 보유 기간 경고: 보유 N일 근접 시 참고용 (tracker 15일 만료와 연동)
HOLDING_DAYS_WARNING_NEAR = 2   # 만료 D일 전부터 ⚠ 보유 기간 N일 근접 (max_hold_days - 이 값)
MAX_HOLD_DAYS_DEFAULT = 15      # 기본 최대 보유 거래일

# 실적/이벤트 전 경고: 실적 발표 D일 이내면 Reason에 참고용 경고
EARNINGS_WARNING_DAYS = 10   # 실적 발표 10일 이내면 ⚠ 실적 발표 N일 전

# BUY/PROMOTED 강등: 실적 발표 N거래일 이내면 WATCH로 강등 (0~N거래일 이내면 적용)
EARNINGS_DOWNGRADE_TRADING_DAYS = 5  # 실적 5거래일 앞두면 BUY/PROMOTED → WATCH_EARNINGS

# 트레일링 스탑 여유 버퍼 (ATR 배수)
SELL_TRAIL_ATR_BUFFER = 0.25

# 보유 일수에 따른 청산 강도 조절: 만료 D일 전부터 트레일 강화 + 컨펌 완화
HOLDING_DAYS_TIGHTEN_BEFORE = 2   # max_hold_days - 이 값 일수 전부터 적용
TRAIL_ATR_MULT_NEAR_EXPIRY = 1.5  # 만료 근접 시 트레일 ATR 배수 (기본 2 대신 타이트하게)

# 강한 매도(음봉 + 거래량 확대): 이 조건이면 컨펌 없이 SELL_TRAIL/SELL_TREND 허용
SELL_STRONG_VOL_DOWN_SKIP_CONFIRM = True
SELL_STRONG_VOL_X = 1.5   # 거래량이 20일평균의 이 배수 이상 + 음봉이면 "강한 매도일"

# 1번: 단계적 청산(스케일 아웃) — T1/T2/T3 도달 시 권장 청산 비율
TP_SCALE_OUT_PCT_T1 = 0.33   # 1차 목표 도달 시 권장 청산 비율
TP_SCALE_OUT_PCT_T2 = 0.33   # 2차 목표 도달 시 추가 권장
TP_SCALE_OUT_PCT_T3 = 1.0    # 3차 또는 전량 = 남은 수량 전부

# 3번: 수익/손실 비대칭 — 손실 시 컨펌 없이 SELL 허용, 손절선 도달 시 전량
SELL_SKIP_CONFIRM_WHEN_LOSS = True   # 손실 구간이면 컨펌 없이 트레일/추세 이탈 시 SELL
# 1차·2차·3차 손절가: 1차=트레일, 2차=중간(-5%), 3차=전액(-10%). 항상 1차 >= 2차 >= 3차.
SELL_2ND_CUT_PCT = 5.0               # 2차 손절가: 진입가 대비 이 % 손실 (중간 손절)
SELL_LOSS_CUT_PCT = 10.0             # 3차 손절가(전액): 진입가 대비 이 % 손실 시 SELL_LOSS_CUT

# 4번: ATR에 따른 트레일 폭 — 확대 시 완화, 축소 시 강화
ATR_TRAIL_LOOKBACK = 10              # 과거 N일 ATR 평균과 비교
ATR_TRAIL_EXPAND_RATIO = 1.2        # 현재 ATR >= 평균*이 비율이면 확대(배수 완화)
ATR_TRAIL_SQUEEZE_RATIO = 0.8       # 현재 ATR <= 평균*이 비율이면 축소(배수 강화)
ATR_TRAIL_MULT_EXPANDED = 2.5       # ATR 확대 시 트레일 ATR 배수
ATR_TRAIL_MULT_SQUEEZED = 1.5       # ATR 축소 시 트레일 ATR 배수


# =========================
# TAKE PROFIT 보수화 옵션
# =========================
TAKE_PROFIT_CONFIRM_ENABLED = True
TAKE_PROFIT_CONFIRM_MODE = "any"   # "any" or "all"

# TP 트리거: High60에 얼마나 근접하면 "익절 후보"로 볼지
TAKE_PROFIT_NEAR_H60 = 0.995  # 0.995 = High60의 99.5% 도달

# (컨펌1) MACD 약화(데드크로스 또는 히스토 0선 하향, 또는 히스토 하락 추세)
TP_CONFIRM_USE_MACD_WEAK = True

# (컨펌2) 거래량 둔화(분배): 오늘 거래량이 20일 평균보다 낮으면
TP_CONFIRM_USE_VOL_FADE = True
TP_CONFIRM_VOL_FADE_X = 0.8   # vol < vol20*0.8

# (컨펌3) 반전 캔들(윗꼬리/음봉 등)
TP_CONFIRM_USE_REVERSAL_CANDLE = True
TP_CONFIRM_MAX_UPPER_WICK = 0.60  # 윗꼬리 비율이 이보다 크면 분배/반전 가능성


# =========================
# RR 개선: base_target = (High20/High60) * (1 + buffer)
# =========================
TARGET_HIGHBUF_20 = 0.02   # 20일 고점 기준 +2%
TARGET_HIGHBUF_60 = 0.03   # 60일 고점 기준 +3%
# ✅ RR 바닥(타겟 최소치): 리스크(Entry-Stop) 기준
TARGET_RR_FLOOR_BREAKOUT = MIN_RR
TARGET_RR_FLOOR_PULLBACK = MIN_RR

TARGET_ATR_DYNAMIC_BUMP_K = 0.35   # 고점이 가까울수록 floor_atr 올리는 강도 (0.3~0.9 권장)
TARGET_ATR_DYNAMIC_CAP = 5.0      # floor_atr 상한 (너무 과대 목표 방지)

DEBUG_RR = True
