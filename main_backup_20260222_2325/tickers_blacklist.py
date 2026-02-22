# tickers_blacklist.py
# 인수/상폐/티커변경/데이터 품질 불량(지속) 등으로 유니버스에서 영구 제외

TICKER_BLACKLIST = {
    # ===== 기존 SKIP LIST =====
    "ANSS", "ATVI", "CDAY", "CMA", "FARO", "HCP", "K", "LTHM",
    "NLOK", "PAYCOM", "SMAR", "SPLK", "WBA", "WORK", "WRK", "ZI",

    # ===== 상장폐지/인수/거래정지 (yfinance 데이터 없음 또는 확인됨) =====
    "ARVL",   # Arrival 상장폐지(2024)
    "ASTR",   # Astra (심볼변경/상장폐지)
    "BIGC",   # BigCommerce 인수
    "BLUE",   # bluebird bio 인수
    "CERN",   # Cerner → Oracle 인수
    "CLEU",   # 상장폐지
    "CYXT",   # 데이터 없음
    "DIDI",   # DiDi 뉴욕 상장폐지
    "FFIE",   # Faraday Future
    "FSR",    # Fisker 상장폐지
    "HZNP",   # Horizon 인수
    "MXIM",   # Maxim → ADI 인수
    "MULN",   # Mullen
    "NLSN",   # Nielsen 인수
    "JNPR",   # Juniper → HPE 인수
    "INFN",   # Infinera 인수
    "NARI",   # Inari 인수
}
