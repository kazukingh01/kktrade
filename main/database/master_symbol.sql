COPY public.master_symbol (symbol_id, symbol_name, exchange, base, currency, is_active, explain) FROM stdin;
0	BTC_JPY	bitflyer	BTC	JPY	t	\N
1	XRP_JPY	bitflyer	XRP	JPY	t	\N
2	ETH_JPY	bitflyer	ETH	JPY	t	\N
3	XLM_JPY	bitflyer	XLM	JPY	f	\N
4	MONA_JPY	bitflyer	MONA	JPY	f	\N
5	FX_BTC_JPY	bitflyer	BTC_FX	JPY	t	\N
6	spot@BTCUSDT	bybit	BTC	USDT	t	\N
7	spot@ETHUSDC	bybit	ETH	USDC	t	\N
8	spot@BTCUSDC	bybit	BTC	USDC	t	\N
9	spot@ETHUSDT	bybit	ETH	USDT	t	\N
10	spot@XRPUSDT	bybit	XRP	USDT	t	\N
11	linear@BTCUSDT	bybit	BTC	USDT	t	\N
12	linear@ETHUSDT	bybit	ETH	USDT	t	\N
13	linear@XRPUSDT	bybit	XRP	USDT	t	\N
14	inverse@BTCUSD	bybit	BTC	USD	t	\N
15	inverse@ETHUSD	bybit	ETH	USD	t	\N
16	inverse@XRPUSD	bybit	XRP	USD	t	\N
17	USDJPY.FOREX	eodhd	USD	JPY	t	\N
18	EURUSD.FOREX	eodhd	EUR	USD	t	\N
19	GBPUSD.FOREX	eodhd	GBP	USD	t	\N
20	USDCHF.FOREX	eodhd	USD	CHF	t	\N
21	AUDUSD.FOREX	eodhd	AUD	USD	t	\N
22	USDCAD.FOREX	eodhd	USD	CAD	t	\N
23	NZDUSD.FOREX	eodhd	NZD	USD	t	\N
24	EURGBP.FOREX	eodhd	EUR	GBP	t	\N
25	EURJPY.FOREX	eodhd	EUR	JPY	t	\N
26	EURCHF.FOREX	eodhd	EUR	CHF	t	\N
27	XAUUSD.FOREX	eodhd	XAU	USD	t	\N
28	GSPC.INDX	eodhd	GSPC	USD	t	S&P 500
29	DJI.INDX	eodhd	DJI	USD	t	Dow Jones
31	BUK100P.INDX	eodhd	BUK100P	GBP	t	Cboe UK 100
32	VIX.INDX	eodhd	VIX	USD	t	CBOE Volatility Index
34	FCHI.INDX	eodhd	FCHI	EUR	t	France 40 index
35	STOXX50E.INDX	eodhd	STOXX50E	EUR	t	Europe 50 index
37	BFX.INDX	eodhd	BFX	EUR	t	Brussels 20 Index
38	IMOEX.INDX	eodhd	IMOEX	RUB	f	Russia 50 Index
39	N225.INDX	eodhd	N225	JPY	t	Nikkei 225
41	SSEC.INDX	eodhd	SSEC		f	\N
42	AORD.INDX	eodhd	AORD	AUD	t	Australlia 500 index
43	BSESN.INDX	eodhd	BSESN	INR	t	india 30 index
44	JKSE.INDX	eodhd	JKSE	IDR	t	Jakarta all Index
51	SPIPSA.INDX	eodhd	SPIPSA	CLP	f	Chile Index
52	MERV.INDX	eodhd	MERV	ARS	f	Argentina  Index
53	TA125.INDX	eodhd	TA125	ILS	f	Israel 125 Index
54	USA500IDXUSD	dukascopy	GSPC	USD	t	S&P 500
55	VOLIDXUSD	dukascopy	VIX	USD	t	CBOE Volatility Index
56	CHIIDXUSD	dukascopy	XIN9	CNY	t	China A50 Index
57	HKGIDXHKD	dukascopy	HSI	HKD	t	Hong Kong 40 Index
58	JPNIDXJPY	dukascopy	N225	JPY	t	Nikkei 225
59	AUSIDXAUD	dukascopy	AXJO	AUD	t	Australlia 200 index
60	INDIDXUSD	dukascopy	NSEI	INR	t	India 50 index
61	SGDIDXSGD	dukascopy	SSGF3	SGD	t	Singapore Blue Chip
62	FRAIDXEUR	dukascopy	FCHI	EUR	t	France 40 index
64	EUSIDXEUR	dukascopy	STOXX50E	EUR	t	Europe 50 index
66	ESPIDXEUR	dukascopy	IBEX	EUR	t	Spain 35 index
65	GBRIDXGBP	dukascopy	BUK100P	GBP	t	UK 100 index
30	IXIC.INDX	eodhd	IXIC	USD	t	NASDAQ Index
33	GDAXI.INDX	eodhd	GDAXI	EUR	t	Germany 40 index
36	N100.INDX	eodhd	N100	EUR	t	Euronext 100 Index
40	HSI.INDX	eodhd	HSI	HKD	t	Hong Kong 40 Index
45	NZ50.INDX	eodhd	NZ50	NZD	t	New Zealand 50 Index
46	KS11.INDX	eodhd	KS11	KRW	t	Koria Index
47	TWII.INDX	eodhd	TWII	TWD	t	Taiwan Index
48	GSPTSE.INDX	eodhd	GSPTSE	CAD	t	Canada 250 Index
49	BVSP.INDX	eodhd	BVSP	BRL	t	Brazil index
50	MXX.INDX	eodhd	GSPTSE	MXN	t	Mexico Index
68	NLDIDXEUR	dukascopy	NL25	EUR	t	Netherlands 25 index
69	PLNIDXPLN	dukascopy	WIG20	PLN	t	Poland 20 index
70	SOAIDXZAR	dukascopy	SA40	ZAR	t	South Africa 40 index
71	USDJPY	dukascopy	USD	JPY	t	\N
72	EURUSD	dukascopy	EUR	USD	t	\N
73	GBPUSD	dukascopy	GBP	USD	t	\N
74	USDCHF	dukascopy	USD	CHF	t	\N
75	AUDUSD	dukascopy	AUD	USD	t	\N
76	USDCAD	dukascopy	USD	CAD	t	\N
77	NZDUSD	dukascopy	NZD	USD	t	\N
78	EURGBP	dukascopy	EUR	GBP	t	\N
79	EURJPY	dukascopy	EUR	JPY	t	\N
80	EURCHF	dukascopy	EUR	CHF	t	\N
81	XAUUSD	dukascopy	XAU	USD	t	\N
82	XAGUSD	dukascopy	XAG	USD	t	\N
63	DEUIDXEUR	dukascopy	GDAXI	EUR	t	Germany 40 index
83	USA500.IDX/USD	dukascopyapi	GSPC	USD	t	S&P 500
84	VOL.IDX/USD	dukascopyapi	VIX	USD	t	CBOE Volatility Index
85	CHI.IDX/USD	dukascopyapi	XIN9	CNY	t	China A50 Index
86	HKG.IDX/HKD	dukascopyapi	HSI	HKD	t	Hong Kong 40 Index
87	JPN.IDX/JPY	dukascopyapi	N225	JPY	t	Nikkei 225
88	AUS.IDX/AUD	dukascopyapi	AXJO	AUD	t	Australlia 200 index
89	IND.IDX/USD	dukascopyapi	NSEI	INR	t	India 50 index
90	SGD.IDX/SGD	dukascopyapi	SSGF3	SGD	t	Singapore Blue Chip
91	FRA.IDX/EUR	dukascopyapi	FCHI	EUR	t	France 40 index
92	DEU.IDX/EUR	dukascopyapi	GDAXI	EUR	t	Germany 40 index
93	EUS.IDX/EUR	dukascopyapi	STOXX50E	EUR	t	Europe 50 index
94	GBR.IDX/GBP	dukascopyapi	BUK100P	GBP	t	UK 100 index
95	ESP.IDX/EUR	dukascopyapi	IBEX	EUR	t	Spain 35 index
97	NLD.IDX/EUR	dukascopyapi	NL25	EUR	t	Netherlands 25 index
98	PLN.IDX/PLN	dukascopyapi	WIG20	PLN	t	Poland 20 index
99	SOA.IDX/ZAR	dukascopyapi	SA40	ZAR	t	South Africa 40 index
100	USD/JPY	dukascopyapi	USD	JPY	t	\N
101	EUR/USD	dukascopyapi	EUR	USD	t	\N
102	GBP/USD	dukascopyapi	GBP	USD	t	\N
103	USD/CHF	dukascopyapi	USD	CHF	t	\N
104	AUD/USD	dukascopyapi	AUD	USD	t	\N
105	USD/CAD	dukascopyapi	USD	CAD	t	\N
106	NZD/USD	dukascopyapi	NZD	USD	t	\N
107	EUR/GBP	dukascopyapi	EUR	GBP	t	\N
108	EUR/JPY	dukascopyapi	EUR	JPY	t	\N
109	EUR/CHF	dukascopyapi	EUR	CHF	t	\N
110	XAU/USD	dukascopyapi	XAU	USD	t	\N
111	XAG/USD	dukascopyapi	XAG	USD	t	\N
67	CHEIDXCHF	dukascopy	SPI20	CHF	t	Switzerland 20 index
96	CHE.IDX/CHF	dukascopyapi	SPI20	CHF	t	Switzerland 20 index
112	AXJO.INDX	eodhd	AXJO	AUD	t	S&P/ASX 200
113	NSEI.INDX	eodhd	NSEI	INR	t	Nifty 50
114	IBEX.INDX	eodhd	IBEX	EUR	t	IBEX 35 Index
115	SPI20.INDX	eodhd	SPI20	CHF	f	SPI 20 PR (Not available)
116	AEX.INDX	eodhd	NL25	EUR	t	AEX Amsterdam Index
117	WIG20.INDX	eodhd	WIG20	PLN	f	Poland 20 index (Not available)
118	XAGUSD.FOREX	eodhd	XAG	USD	t	\N
\.
