# tickers_universe.py
# -----------------------------
# 유니버스
# -----------------------------

BASE_TICKERS = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AVGO","JPM","XOM",
    "LLY","V","MA","COST","HD","PG","NFLX","AMD","INTC","CSCO",
    "ORCL","CRM","ADBE","QCOM","TXN","BAC","GS","MS","ABBV","MRK",
    "DHR","ISRG","CAT","BA","DIS","NKE","MCD","SBUX","WMT","CVX","NOW","PANW","ANET","MU","TSM"
]

MID_CAP_50_100T_KRW = [
    "KMI","WBD","TRV","PCAR","LHX","AEP","PSX","FTNT","APD","VLO",
    "AJG","EOG","ROST","AFL","AZO","MPC","NET","DLR","MPWR",
    "BDX","BKR","O","SNOW","GWW","URI","SRE","MPLX","ZTS","F","LITE","CRDO","CLS"
]

SEMI_AI = [
    "SMCI","ARM","PLTR","SNOW","NET","DDOG","MDB","ZS","CRWD","TEAM",
    "SHOP","U","RBLX","DOCU","OKTA","PATH","ESTC","HUBS","TTD","SNDK"
]

CYCLICAL = [
    "CAT","DE","ETN","PH","HON","LHX","GD","NOC","BA","URI",
    "SLB","HAL","OXY","EOG","MPC","PSX","VLO","COP","CVX","XOM"
]

DEFENSIVE_GROWTH = [
    "LLY","UNH","JNJ","ABBV","MRK","ISRG","DHR","BSX","SYK","ZTS",
    "COST","WMT","HD","LOW","MCD","SBUX","NKE","PG","KO","PEP","ARQT"
]

# 금융주만 (ETF 제외)
FIN_STOCKS = [
    "JPM","GS","MS","BAC","BLK","AXP","SPGI","ICE","CME","SCHW",
]

# 유니버스에서 제외할 ETF (최종 TICKERS에서 필터링)
ETF_EXCLUDE = frozenset({
    "SPY","QQQ","IWM","DIA","XLK","XLE","XLF","XLV","XLI","XLP","XLY","XLB","XLU","XLRE","XLC",
    "SMH","SOXX","ARKK","VGT","IGV","HACK","SKYY","FDN","IWY","VUG","IVW","QUAL","MTUM",
    "SCHA","VB","VXF","VTWO","IJR","SPSM","SLY","RUT","RTY","TNA","TZA","URTY","SRTY",
    "LABU","LABD","TECL","TECS","SOXL","SOXS","FNGU","FNGD","TQQQ","SQQQ","QLD","PSQ",
    "SPUU","SPXL","UPRO","SDS","SPXS","SH","RSP","EQAL","EQUAL","SPYV","IVE","IWF","IWD","IWR",
    "VTV","VOE","VOT","RPV","RFV","IWS","IWN","VTWV","VBR","RZV","VBK","VO","VHG","VFH",
    "VDC","VCR","VIS","VAW","VDE","VOX","VNQ","VNQI","VHT","VGH","BND","BNDX","AGG","LQD",
    "HYG","JNK","TIP","TIPX","VTIP","SCHP","LTPZ","EDV","TLT","IEF","SHY","GOVT","FLOT","FLTR",
    "MUB","PZA","TFI","HYD","EMB","PCY","CEW","FXF","FXE","FXB","FXY","FXA","UUP","UDN",
    "GLD","SLV","IAU","GDX","GDXJ","COPX","CPER","DBA","DBC","USO","UNG","WEAT","CORN","SOYB",
    "CANE","NIB","JO","BAL","COT","CTNN","VNM","EWZ","EWW","ECH","EPU","GXG","ILF","EEM",
    "IEMG","VWO","SCHE","EWY","EWT","EWM","EWS","EPHE","EIDO","INDA","THD","KRE","KBE","KIE","KCE",
    "XBI","IBB","BBH","PPA","ITA","JETS","XHB","ITB","PKB","PBE","IGN","IYT","SEA","AIRR","BJK",
    "TAN","ICLN","QCLN","PBW","ACES","CTEC","LIT","BATT","DRIV","IDRV","CLOU","WCLD","BUG","AIQ","CHAT",
    "BLCN","BLOK","BITS","BITQ","QQQJ","QQQM","BOTZ","ROBO","IRBO","LEGR",
    "QQQX","JEPI","JEPQ","XYLD","XYLE","DIVO","SCHD","VYM","DVY","HDV","SPHD","SPYD","FVD","FDL",
    "DHS","DTD","FBT","XHE","XHS","IHI","PSJ","PJP","GNOM","ARKG","PBE","IDNA","BTEC","ROBT",
    "ROM","RXL","SSO","UMDD","MVV","HCXY","SVOL","RYLD","QYLD","XSD",
})

AI_TECH_EXPANSION = [
"ADSK","CDNS","SNPS","INTU","PAYC","PAYX","WDAY","DDOG","ZS","CRWD","OKTA","NET","MDB","ESTC",
"FSLY","DOCN","HCP","GTLB","CFLT","AI","BILL","S","IOT","SPLK","DT","TWLO","ZI","APP","AFRM",
"UPST","COIN","MSTR","RIOT","MARA","CLS","ONTO","AMBA","LSCC","RMBS","FORM","MKSI","ACLS",
"VECO","KLIC","IPGP","LRCX","KLAC","ENTG","AMAT","TER","SWKS","QRVO","MPWR","WOLF","ON","NXPI",
"ADI","MCHP","STM","HIMX","OLED","SYNA","POWI","SMTC","DIOD","ALGM","SITM","NVTS","AEHR","CEVA",
"FARO","CGNX","KEYS","ANSS","PTC","TYL","MANH","PAYCOM","SSNC","GWRE","VEEV","ZM","PCOR",
"TTWO","EA","ATVI","ROKU","SPOT","DASH","ABNB","UBER",
"LYFT","SNOW","PANW","FTNT","CHKP","TENB","VRNS","CYBR","QLYS","SAIL","GEN","NLOK",
"AKAM","FAST","CDAY","IT","GLOB","EPAM","FIVN","PD","BOX","SMAR","ASAN","WORK","KVYO"
]


LARGE_CAP_EXPANSION = [
"ROP","TT","ITW","EMR","ROK","PHM","LEN","DHI","NVR","POOL","MAS","AVY","PKG","WRK","BALL",
"IFF","LIN","APTV","BWA","ALB","LTHM","FMC","CF","MOS","NTR","ADM","BG","DAR","HRL",
"TSN","K","GIS","CPB","SJM","HSY","MDLZ","CLX","KMB","CHD","EL","ULTA","DG","DLTR","BBY","TSCO",
"ORLY","AZO","AAP","GPC","LKQ","PAG","AN","SAH","KMX","CVNA","EXPE","BKNG","RCL","CCL","NCLH",
"MAR","HLT","HST","DRI","YUM","YUMC","CMG","DPZ","WING","SHAK","TXRH","CAVA","LULU","CROX","DECK",
"CPRT","FND","HD","LOW","TGT","BJ","KR","ACI","UNFI","WBA","CI","ELV","HUM","CNC","MOH","UHS",
"HCA","THC","DVA","LH","DGX","TECH","ICLR","PODD","DXCM","ALGN","IDXX","ZBH","XRAY","MTD",
"WAT","BIO","BRKR","A","TMO","ILMN","GH","NTRA","EXAS","MRNA","BNTX","NBIX","INCY","REGN"
]

VALUE_DIVIDEND_EXPANSION = [
"MET","PRU","AIG","ALL","PGR","CB","TRV","WRB","CINF","HIG","AJG","MMC","AON","BRO","NDAQ",
"MSCI","MCO","SPGI","ICE","CBOE","BX","KKR","APO","ARES","OWL","RJF","SF","FHN","CFG","KEY",
"RF","HBAN","ZION","CMA","FITB","USB","PNC","TFC","MTB","WAL","OZK","ETR","FE","AEE","DTE",
"ED","PEG","EXC","XEL","WEC","ES","PPL","ATO","CMS","NI","NRG","NEE","DUK","SO"
]

AI_TECH_EXPANSION_EXTRA = [
"ACN","IBM","HPQ","DELL","NTAP","HPE","STX","WDC","LOGI","SONO",
"GRMN","VRSN","FICO","JKHY","LDOS","SAIC","CACI","BAH","PAR","QTWO",
"PEGA","BLKB","CSGS","KN","CRNC","RPD","NICE","VRNT","DOX","OTEX",
"TDC","INFA","ALTR","VERX","FRSH","APPF","PAYA","FOUR","GPN","WEX",
"FLT","BR","J","KBR","MRCY","OSIS","PLXS","SANM","TTMI","COHU",
"UCTT","IIVI","ENPH","SEDG","ARRY","NXT","RUN","FSLR","AMKR","ASX",
"UMC","PINS","SNAP","MTCH","BMBL","YELP","TRIP","ZG","Z","OPRA",
"RAMP","IAS","PERI","PUBM","MGNI","APPS","BELFB","CAMT","NVMI",
"ICHR","PDFS","IMMR","KOPN","VIAV","LITE","AAOI","INFN","CIEN",
"COMM","EXTR","JNPR","CALX","CMBM","UI","RXT","NABL","PDCO",
"SWI","INOD","RDWR","SPNS","MLNK","SCSC","CNXN","ARLO","MAXN","DSP",
]
CLOUD_SOFTWARE_EXTRA = [
"ORAN","ERIC","NOK","SAP","TEAM","SQ","ADP","WDAY","PAYX","PAYC",
"INTA","DOCS","AMN","RXRX","SPSC","NCNO","AVPT","NEWR","CXM","SMWB",
"RNG","BAND","EGHT","AVDX","ARCE","COUR","UDMY","DUOL","PLXS","GLBE",
"BASE","MNDY","S","AI","GDRX","SEM","QDEL","OMCL","BL","PSFE",
"TOST","ZIP","DOCN","CRSR","IAC","ANGI","MTTR","OPEN","HIMS","TRUE",
"CYXT","ALKT","SPT","WK","VRNS","SCWX","BLZE","BIGC","PD","AMPL",
"SEG","OCFT","ACIW","EVBG","PAYO","RSKD","SQSP","STNE","DLO","LAW",
"RELY","KTOS","DOMO","AYX","QSI","SDGR",
]

PLATFORM_SOFTWARE_ADDITIONAL = [
"SAP","ADP","SQ","SHOP","SE","WDAY","VRSK","ANSS","CDNS","SNPS",
"TYL","PTC","GWRE","MANH","SSNC","PAYX","PAYC","INTU","PEGA","BLKB",
"NICE","VRNT","DOX","OTEX","TDC","INFA","ALTR","VERX","FRSH","APPF","ORCL","BABA",
]

MACRO_REGIME_EXPANSION = [
# 운송 / 산업재 선행 (경기 초기 감지)
"UNP","CSX","NSC","FDX","UPS","LUV","DAL","UAL","ALK","JBLU",
"ODFL","JBHT","CHRW","EXPD","HUBG","KEX","KNX","WERN","SAIA","ARCB",
"GXO","R","RYAN","PAC","CPA","SKYW","ALGT","MATX","HTLD","SNDR",
"HII","TXT","CW","HEI","TDG","RRX","AME","XYL","IEX","PNR",
"DOV","SWK","ITW","IR","ROP","EME","LECO","GGG","WSO","FLS",
"WTS","AYI","HUBB","FELE","AOS","AGCO","BC","CMI","TTC","OSK",

# 유틸리티 / 금리민감
"NEE","DUK","SO","D","EXC","XEL","PCG","EIX","ES","WEC",
"ED","AEE","ATO","CMS","NI","PNW","NRG","EVRG","LNT","IDA",
"OGE","PPL","PEG","SRE","ETR","FE","HE","AES","AWK","CNP",
"AEP","AGR","BKH","OGS","SR","ORA","NWN","MGEE","ELP",
"UGI","OTTR","PNM","HELE","SWX","AVA","NEP","AY","BEPC","BEP",

# 리츠 / 금리하락 선행
"PLD","AMT","CCI","EQIX","PSA","SPG","O","DLR","WELL","VTR",
"EQR","AVB","ESS","MAA","UDR","CPT","ARE","KIM","FRT",
"REG","BRX","AKR","ROIC","SITC","PEAK","DOC","HR","MPW","NHI",
"SBRA","CTRE","NSA","CUBE","EXR","LSI","PK","DRH","RHP",
"APLE","SHO","BXP","SLG","VNO","HIW","KRC","DEI","OFC","JBGS",
"PDM","ELME","CUZ",

# 지역은행 / 금융 스트레스 해소
"HBAN","RF","KEY","CFG","CMA","ZION","FHN","OZK","SF","ONB",
"FNB","WBS","WAL","PB","UBSI","TCBI","CVBF","HOPE","BANC","BKU",
"CATY","EWBC","PACW","BPOP","INDB","FFIN","BOKF","WTFC","ASB","ABCB",
"BANR","FBP","FULT","IBOC","NBHC","STBA","TRMK","WSFS","HTLF","UFPI",
"NWBI","MCBS","FCF","FFBC","TCBK","SRCE","FBK","HFWA","LOB","AMAL",
"OCFC","CPF","BHLB",

# 필수소비 / 침체 방어
"CL","KMB","KHC","HSY","MDLZ","PEP","KO","KR","COST","WMT",
"DG","DLTR","TSCO","ACI","UNFI","CPB","SJM","GIS","K","HRL",
"MKC","CAG","POST","SFM","GO","INGR","DAR","FLO","BGS","HAIN",
"CALM","SENEA","THS","USFD","CWST","FIZZ","PRMW","VITL","SMPL",

# 소재 / 화학 / 사이클 상단
"DD","DOW","PPG","SHW","LYB","EMN","FMC","ALB","CF","MOS",
"NTR","IPI","UAN","OLN","WLK","HUN","ASH","CC","CE","SCL",
"KWR","NEU","AVNT","GPRE","ADM","BG","TSN","PPC","SAND",
"MP","SBSW","HL","AG","PAAS","CDE","EXK","OR","SVM",

]

RUSSELL_MIDCAP_TOP50_ADD = [
    "TTWO","ROKU","HUBB","BR","VRSK","CDAY","FTV","ANSS","TYL","PTC",
    "MANH","GWRE","SSNC","NICE","DOX","OTEX","BLKB","ALTR","APPF","VERX",
    "FDS","MKTX","NDAQ","CBOE","RJF","BEN","IVZ","SEIC","CINF","WRB",
    "RLI","KNSL","AFG","SIGI","ORI","FNF","RDN","MTG","ESNT","AXS",
    "VOYA","PRI","LNC","AIZ","UNM","AGO","HLI","MC","SFBS"
]

# 나스닥/뉴욕증시 추가 500 (리스트 내 중복 제거)
NASDAQ_NYSE_500 = [
    "BRK-B","IONQ","RGTI","QUBT","ASTS","RKLB","LUNR","SPCE","BKSY","SATL",
    "RIVN","LCID","FSR","NKLA","GOEV","BLNK","CHPT","EVGO","VLDR","LAZR",
    "OUST","LIDR","AEYE","AEVA","INVZ","IMMR","MVIS","PLUG","BE","QS",
    "RIDE","HYLN","XPEV","LI","NIO","FUV","WKHS","ARVL","REE","ZEV",
    "SOLO","FFIE","MULN","BOLT","BBAI","BFRG","SOUN","PRCH","GFAI","BOTZ",
    "ROBO","IRBO","BLCN","LEGR","QQQJ","QQQM","VGT","IGV","HACK","SKYY",
    "FDN","IWY","VUG","IVW","QUAL","MTUM","SCHA","VB","VXF","VTWO",
    "IJR","IWM","SPSM","SLY","RUT","RTY","TNA","TZA","URTY","SRTY",
    "LABU","LABD","TECL","TECS","SOXL","SOXS","FNGU","FNGD","TQQQ","SQQQ",
    "QLD","PSQ","SPUU","SPXL","UPRO","SDS","SPXS","SH","RSP","EQAL",
    "EQUAL","SPYV","IVE","IWF","IWD","IWR","VTV","VOE","VOT","RPV",
    "RFV","IWS","IWN","VTWV","VBR","RZV","VBK","VO","VHG","VFH",
    "VDC","VCR","VIS","VAW","VDE","VOX","VNQ","VNQI","VHT","VGH",
    "BND","BNDX","AGG","LQD","HYG","JNK","TIP","TIPX","VTIP","SCHP",
    "LTPZ","EDV","TLT","IEF","SHY","GOVT","FLOT","FLTR","MUB","PZA",
    "TFI","HYD","EMB","PCY","CEW","FXF","FXE","FXB","FXY","FXA",
    "UUP","UDN","GLD","SLV","IAU","GDX","GDXJ","COPX","CPER","DBA",
    "DBC","USO","UNG","WEAT","CORN","SOYB","CANE","NIB","JO","BAL",
    "COT","CTNN","VNM","EWZ","EWW","ECH","EPU","GXG","ILF","EEM",
    "IEMG","VWO","SCHE","EWY","EWT","EWM","EWS","EPHE","EIDO","INDA",
    "THD","KRE","KBE","KIE","KCE","XLF","XLK","XLE","XLV","XLI",
    "XLP","XLY","XLB","XLU","XLRE","XLC","XBI","IBB","BBH","PPA",
    "ITA","JETS","XHB","ITB","PKB","PBE","IGN","IYT","SEA","AIRR",
    "BJK","TAN","ICLN","QCLN","PBW","ACES","CTEC","LIT","BATT","DRIV",
    "IDRV","CLOU","WCLD","BUG","AIQ","CHAT","BLOK","BITS","BITQ","MSTR",
    "COIN","RIOT","MARA","CLSK","CIFR","BTBT","BITF","HUT","CLEU","CORZ",
    "MIGI","SDIG","IREN","HIVEL","BTDR","APLD","HIVE","HOOD","SOFI","AFRM",
    "UPST","OPEN","LC","OPFI","LPRO","ML","NU","PAGS","GPN","ADYEN",
    "STNE","MELI","SE","GRAB","CPNG","BABA","JD","PDD","VIPS","TME",
    "BIDU","NTES","BILI","IQ","TAL","EDU","FUTU","TIGR","LX","DIDI",
    "GOCO","BROS","SWAV","PODD","SENS","DXCM","TNDM","ALGM","INMD","NARI",
    "PEN","ITGR","SRDX","ATRC","SILK","ICUI","ZBH","XRAY","BAX","BSX",
    "EW","HOLX","RMD","IDXX","MTD","TECH","DHR","WAT","TMO","A",
    "BIO","BRKR","ILMN","EXAS","NTRA","NEO","QGEN","ICLR","DGX","LH",
    "HCA","CYH","THC","SEM","AMED","CHE","PDCO","HSIC","OMI","RDNT",
    "DVA","FMS","OPCH","CVS","MCK","CAH","ABC","COR","JAZZ","SGEN",
    "BIIB","ALNY","BMRN","RARE","SRPT","FOLD","UTHR","HZNP","INCY","EXEL",
    "LGND","ACAD","SUPN","SAGE","BCRX","RGNX","BLUE","EDIT","CRSP","NTLA",
    "BEAM","VERV","PRME","VCYT","NSTG","QURE","RXRX","PACB","GH","TDOC",
    "HIMS","LFST","DOCS","AMWL","OMCL","ZBRA","HOLI","GNSS","SWIR","LITE",
    "IIVI","COHR","IPGP","MKSI","VIAV","IMOS","CAMT","KLIC","FORM","AEHR",
    "ACLS","DIOD","POWI","SMTC","SYNA","MCHP","MXIM","ADI","TXN","MRVL",
    "LSCC","RMBS","CDNS","SNPS","NXPI","ON","WOLF","CRUS","QRVO","SWKS",
    "SIMO","GFS","AVGO","CEVA","DSP","WDC","STX","MU","DOCN","DDOG",
    "NET","MDB","ESTC","CFLT","GTLB","PCTY","BILL","ZUO","SMAR","ASAN",
    "BASE","MNDY","VEEV","ZM","DOCU","HUBS","NCNO","AVPT","RNG","BAND",
    "FIVN","EGHT","CVLT","COMM","BRBR","IT","GDDY","AKAM","Z","ZG",
    "IAC","ANGI","REDFIN","RDFN","COMP","REAX","RE/MAX","CORT","SRNE","VRTX",
    "REGN","MRNA","BNTX","GILD","AMGN","BLDP","FCEL","HY","NOVA","RUN",
    "SEDG","ENPH","FSLR","MAXN","SPWR","CSIQ","JKS","DQ","SOL","SUNW",
    "ALB","LTHM","LAC","LITM","MP","PLL","WIX","SQSP","CARG","FROG",
    "APPS","VERB",
]

# 나스닥/뉴욕증시 추가 PART2 (리스트 내 중복 제거)
NASDAQ_NYSE_500_PART2 = [
    "VKTX","KRYS","RCKT","PTCT","CELH","SWAV","FROG","CARG","WIX","VERB",
    "GOCO","BROS","NU","ML","LPRO","OPFI","PAGS","TIGR","LX","FUTU",
    "EDU","TAL","BILI","IQ","TME","VIPS","NTES","CPNG","DIDI","ASML",
    "NVO","AZN","SNY","SAN","GSK","NVS","ROG","LYB","CE","EMN",
    "FMC","PPG","SHW","DD","DOW","CF","MOS","VST","CEG","VSTO",
    "CWEN","NOVA","OPAL","ORA","ENR","EVRG","AES","AEE","LNT","WEC",
    "AWK","CNP","DTE","ES","SO","PEG","ED","XEL","EIX","PCG",
    "AEP","EXC","NRG","OTTR","MGEE","PNM","HELE","SWX","AVA","NEP",
    "BEPC","BEP","AY","OGS","BKH","AGR","NWN","UGI","SR","PNW",
    "IDA","OGE","PPL","JKHY","FIS","GPN","FISV","ADP","PAYX","PAYC",
    "WDAY","EFX","TRU","CBOE","NDAQ","MKTX","CME","ICE","MSCI","SPGI",
    "MCO","FDS","DV","INFO","NLSN","IQV","CERN","CTSH","EPAM","IT",
    "GLOB","DXC","ACN","IBM","HPQ","DELL","HPE","NTAP","WDC","STX",
    "LOGI","SONO","GRMN","VRSN","FICO","LDOS","SAIC","CACI","BAH","PAR",
    "QTWO","CSGS","KN","CRNC","RPD","PAYA","FOUR","WEX","FLT","BR",
    "J","KBR","MRCY","OSIS","PLXS","SANM","TTMI","COHU","UCTT","IIVI",
    "ENPH","SEDG","ARRY","NXT","RUN","FSLR","AMKR","ASX","UMC","PINS",
    "SNAP","MTCH","BMBL","YELP","TRIP","ZG","Z","OPRA","RAMP","IAS",
    "PERI","PUBM","MGNI","APPS","BELFB","CAMT","NVMI","ICHR","PDFS","IMMR",
    "KOPN","VIAV","LITE","AAOI","INFN","CIEN","COMM","EXTR","JNPR","CALX",
    "CMBM","UI","RXT","NABL","PDCO","SWI","INOD","RDWR","SPNS","MLNK",
    "SCSC","CNXN","ARLO","MAXN","DSP","WOLF","ON","NXPI","ADI","MCHP",
    "STM","HIMX","OLED","SYNA","POWI","SMTC","DIOD","ALGM","SITM","NVTS",
    "AEHR","CEVA","FARO","CGNX","KEYS","ANSS","PTC","TYL","MANH","PAYCOM",
    "SSNC","GWRE","VEEV","ZM","PCOR","TTWO","EA","ATVI","ROKU","SPOT",
    "DASH","ABNB","UBER","LYFT","SNOW","PANW","FTNT","CHKP","TENB","VRNS",
    "CYBR","QLYS","SAIL","GEN","NLOK","AKAM","FAST","CDAY","FIVN","PD",
    "BOX","SMAR","ASAN","WORK","KVYO","RGTI","QUBT",
]

# 나스닥/뉴욕증시 추가 PART3 (리스트 내 중복 제거)
NASDAQ_NYSE_500_PART3 = [
    "ASML","NVO","AZN","SNY","SAN","GSK","NVS","ROG","LYB","CE",
    "VST","CEG","VSTO","CWEN","OPAL","ORA","ENR","JKHY","FIS","FISV",
    "EFX","TRU","DV","INFO","NLSN","IQV","CERN","CTSH","DXC","GRAB",
    "RBLX","U","DDOG","SNOW","NET","MDB","ZS","CRWD","OKTA","PATH",
    "ESTC","TEAM","DOCU","HUBS","TTD","SNDK","SMCI","ARM","PLTR","SHOP",
    "QQQX","JEPI","JEPQ","XYLD","XYLE","DIVO","SCHD","VYM","DVY","HDV",
    "SPHD","SPYD","FVD","FDL","DHS","DTD","FBT","IBB","XBI","XHE",
    "XHS","IHI","PSJ","PJP","BBH","GNOM","ARKG","XLV","VHT","PBE",
    "IDNA","BTEC","ROBT","BOTZ","IRBO","ROBO","TECL","SOXL","TQQQ","UPRO",
    "SPXL","QLD","SSO","UMDD","MVV","URTY","TNA","ROM","RXL","UXIN",
    "EH","DNUT","BROS","FAT","FATBB","CAVA","WING","SHAK","DPZ","PZZA",
    "YUM","YUMC","CMG","TXRH","BLMN","DRI","EAT","DARDEN","MCD","WEN",
    "JACK","BOJA","ARCO","TACO","LOCO","CABO","PTLO","DENN","NDLS","BURGER",
]

# 나스닥/뉴욕증시 추가 PART4 (기존과 중복 없는 티커만)
NASDAQ_NYSE_500_PART4 = [
    "HCXY","SVOL","RYLD","QYLD","GROM","LLAP","FLBR","FLCH","FLEH","FLIN",
    "FLIY","FLJP","FLKR","FLLA","FLTW","GLIN","KEMD","KEME","KEMG","KEMH",
    "KEMJ","KEMK","KEML","KEMN","KEMQ","KEMX","XSD",
]

# 추천 추가 100종목 (제약/방산/에너지/소비재/금융/자동차/통신/리츠/철강/골드 등)
RECOMMENDED_ADD_100 = [
    "PFE","ABT","BMY","LMT","RTX","GE","MDT","PXD","DVN","HES","FANG","MRO","PYPL","BK","STLA",
    "GM","PM","MO","OKE","WMB","ET","EPD","TJX","CMCSA","T","VZ","TMUS","CHTR","PARA","LYV",
    "ECL","CARR","OTIS","JCI","WM","RSG","CLH","HAS","MAT","H","IP","AMCR","BERY","GPK","SON",
    "SLGN","MHK","FBHS","JELD","TREX","BURL","WSM","RH","ETH","LZB","DDS","KSS","M","JWN","GPS",
    "ANF","AEO","SIG","BOOT","SCVL","FIVE","BGFV","NCR","CDW","FFIV","FCX","NEM","GOLD","FNV","WPM","RGLD",
    "LEG","MTH","MHO","MDC","TPH","SKY","INVH","SUI","AAL","NUE","STLD","RS","CLF","X","ATI","BTI","LW","KGC","IAG","GFI","AUY","AGI","EGO","SA",
]

# 최종 유니버스: 중복 제거(첫 등장만 유지) + ETF 제외 + 정렬
_all_raw = (
    BASE_TICKERS
    + MID_CAP_50_100T_KRW
    + SEMI_AI
    + CYCLICAL
    + DEFENSIVE_GROWTH
    + FIN_STOCKS
    + AI_TECH_EXPANSION
    + AI_TECH_EXPANSION_EXTRA
    + CLOUD_SOFTWARE_EXTRA
    + PLATFORM_SOFTWARE_ADDITIONAL
    + LARGE_CAP_EXPANSION
    + VALUE_DIVIDEND_EXPANSION
    + MACRO_REGIME_EXPANSION
    + RUSSELL_MIDCAP_TOP50_ADD
    + NASDAQ_NYSE_500
    + NASDAQ_NYSE_500_PART2
    + NASDAQ_NYSE_500_PART3
    + NASDAQ_NYSE_500_PART4
    + RECOMMENDED_ADD_100
)
# 대소문자 통일 후 첫 등장만 유지(중복 제거)
_unique_ordered = list(dict.fromkeys(t.upper().strip() for t in _all_raw))
TICKERS = sorted(t for t in _unique_ordered if t not in ETF_EXCLUDE)
