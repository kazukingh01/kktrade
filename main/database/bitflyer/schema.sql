--
-- Name: bitflyer_executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_executions (
    symbol smallint NOT NULL,
    id bigint NOT NULL,
    side smallint NOT NULL,
    unixtime bigint,
    price real,
    size real
);

ALTER TABLE public.bitflyer_executions OWNER TO postgres;


--
-- Name: bitflyer_fundingrate; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_fundingrate (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    current_funding_rate real,
    next_funding_rate_settledate bigint
);

ALTER TABLE public.bitflyer_fundingrate OWNER TO postgres;


--
-- Name: bitflyer_orderbook; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_orderbook (
    symbol smallint NOT NULL,
    unixtime bigint NOT NULL,
    side smallint NOT NULL,
    price real,
    size real
) PARTITION BY RANGE (unixtime);


ALTER TABLE public.bitflyer_orderbook OWNER TO postgres;

CREATE TABLE bitflyer_orderbook_201901 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1546300800) TO (1548979200);
CREATE TABLE bitflyer_orderbook_201902 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1548979200) TO (1551398400);
CREATE TABLE bitflyer_orderbook_201903 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1551398400) TO (1554076800);
CREATE TABLE bitflyer_orderbook_201904 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1554076800) TO (1556668800);
CREATE TABLE bitflyer_orderbook_201905 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1556668800) TO (1559347200);
CREATE TABLE bitflyer_orderbook_201906 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1559347200) TO (1561939200);
CREATE TABLE bitflyer_orderbook_201907 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1561939200) TO (1564617600);
CREATE TABLE bitflyer_orderbook_201908 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1564617600) TO (1567296000);
CREATE TABLE bitflyer_orderbook_201909 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1567296000) TO (1569888000);
CREATE TABLE bitflyer_orderbook_201910 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1569888000) TO (1572566400);
CREATE TABLE bitflyer_orderbook_201911 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1572566400) TO (1575158400);
CREATE TABLE bitflyer_orderbook_201912 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1575158400) TO (1577836800);
CREATE TABLE bitflyer_orderbook_202001 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1577836800) TO (1580515200);
CREATE TABLE bitflyer_orderbook_202002 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1580515200) TO (1583020800);
CREATE TABLE bitflyer_orderbook_202003 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1583020800) TO (1585699200);
CREATE TABLE bitflyer_orderbook_202004 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1585699200) TO (1588291200);
CREATE TABLE bitflyer_orderbook_202005 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1588291200) TO (1590969600);
CREATE TABLE bitflyer_orderbook_202006 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1590969600) TO (1593561600);
CREATE TABLE bitflyer_orderbook_202007 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1593561600) TO (1596240000);
CREATE TABLE bitflyer_orderbook_202008 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1596240000) TO (1598918400);
CREATE TABLE bitflyer_orderbook_202009 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1598918400) TO (1601510400);
CREATE TABLE bitflyer_orderbook_202010 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1601510400) TO (1604188800);
CREATE TABLE bitflyer_orderbook_202011 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1604188800) TO (1606780800);
CREATE TABLE bitflyer_orderbook_202012 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1606780800) TO (1609459200);
CREATE TABLE bitflyer_orderbook_202101 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1609459200) TO (1612137600);
CREATE TABLE bitflyer_orderbook_202102 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1612137600) TO (1614556800);
CREATE TABLE bitflyer_orderbook_202103 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1614556800) TO (1617235200);
CREATE TABLE bitflyer_orderbook_202104 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1617235200) TO (1619827200);
CREATE TABLE bitflyer_orderbook_202105 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1619827200) TO (1622505600);
CREATE TABLE bitflyer_orderbook_202106 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1622505600) TO (1625097600);
CREATE TABLE bitflyer_orderbook_202107 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1625097600) TO (1627776000);
CREATE TABLE bitflyer_orderbook_202108 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1627776000) TO (1630454400);
CREATE TABLE bitflyer_orderbook_202109 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1630454400) TO (1633046400);
CREATE TABLE bitflyer_orderbook_202110 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1633046400) TO (1635724800);
CREATE TABLE bitflyer_orderbook_202111 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1635724800) TO (1638316800);
CREATE TABLE bitflyer_orderbook_202112 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1638316800) TO (1640995200);
CREATE TABLE bitflyer_orderbook_202201 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1640995200) TO (1643673600);
CREATE TABLE bitflyer_orderbook_202202 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1643673600) TO (1646092800);
CREATE TABLE bitflyer_orderbook_202203 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1646092800) TO (1648771200);
CREATE TABLE bitflyer_orderbook_202204 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1648771200) TO (1651363200);
CREATE TABLE bitflyer_orderbook_202205 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1651363200) TO (1654041600);
CREATE TABLE bitflyer_orderbook_202206 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1654041600) TO (1656633600);
CREATE TABLE bitflyer_orderbook_202207 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1656633600) TO (1659312000);
CREATE TABLE bitflyer_orderbook_202208 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1659312000) TO (1661990400);
CREATE TABLE bitflyer_orderbook_202209 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1661990400) TO (1664582400);
CREATE TABLE bitflyer_orderbook_202210 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1664582400) TO (1667260800);
CREATE TABLE bitflyer_orderbook_202211 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1667260800) TO (1669852800);
CREATE TABLE bitflyer_orderbook_202212 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1669852800) TO (1672531200);
CREATE TABLE bitflyer_orderbook_202301 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1672531200) TO (1675209600);
CREATE TABLE bitflyer_orderbook_202302 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1675209600) TO (1677628800);
CREATE TABLE bitflyer_orderbook_202303 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1677628800) TO (1680307200);
CREATE TABLE bitflyer_orderbook_202304 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1680307200) TO (1682899200);
CREATE TABLE bitflyer_orderbook_202305 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1682899200) TO (1685577600);
CREATE TABLE bitflyer_orderbook_202306 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1685577600) TO (1688169600);
CREATE TABLE bitflyer_orderbook_202307 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1688169600) TO (1690848000);
CREATE TABLE bitflyer_orderbook_202308 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1690848000) TO (1693526400);
CREATE TABLE bitflyer_orderbook_202309 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1693526400) TO (1696118400);
CREATE TABLE bitflyer_orderbook_202310 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1696118400) TO (1698796800);
CREATE TABLE bitflyer_orderbook_202311 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1698796800) TO (1701388800);
CREATE TABLE bitflyer_orderbook_202312 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1701388800) TO (1704067200);
CREATE TABLE bitflyer_orderbook_202401 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1704067200) TO (1706745600);
CREATE TABLE bitflyer_orderbook_202402 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1706745600) TO (1709251200);
CREATE TABLE bitflyer_orderbook_202403 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1709251200) TO (1711929600);
CREATE TABLE bitflyer_orderbook_202404 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1711929600) TO (1714521600);
CREATE TABLE bitflyer_orderbook_202405 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1714521600) TO (1717200000);
CREATE TABLE bitflyer_orderbook_202406 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1717200000) TO (1719792000);
CREATE TABLE bitflyer_orderbook_202407 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1719792000) TO (1722470400);
CREATE TABLE bitflyer_orderbook_202408 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1722470400) TO (1725148800);
CREATE TABLE bitflyer_orderbook_202409 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1725148800) TO (1727740800);
CREATE TABLE bitflyer_orderbook_202410 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1727740800) TO (1730419200);
CREATE TABLE bitflyer_orderbook_202411 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1730419200) TO (1733011200);
CREATE TABLE bitflyer_orderbook_202412 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1733011200) TO (1735689600);
CREATE TABLE bitflyer_orderbook_202501 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1735689600) TO (1738368000);
CREATE TABLE bitflyer_orderbook_202502 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1738368000) TO (1740787200);
CREATE TABLE bitflyer_orderbook_202503 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1740787200) TO (1743465600);
CREATE TABLE bitflyer_orderbook_202504 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1743465600) TO (1746057600);
CREATE TABLE bitflyer_orderbook_202505 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1746057600) TO (1748736000);
CREATE TABLE bitflyer_orderbook_202506 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1748736000) TO (1751328000);
CREATE TABLE bitflyer_orderbook_202507 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1751328000) TO (1754006400);
CREATE TABLE bitflyer_orderbook_202508 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1754006400) TO (1756684800);
CREATE TABLE bitflyer_orderbook_202509 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1756684800) TO (1759276800);
CREATE TABLE bitflyer_orderbook_202510 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1759276800) TO (1761955200);
CREATE TABLE bitflyer_orderbook_202511 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1761955200) TO (1764547200);
CREATE TABLE bitflyer_orderbook_202512 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1764547200) TO (1767225600);
CREATE TABLE bitflyer_orderbook_202601 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1767225600) TO (1769904000);
CREATE TABLE bitflyer_orderbook_202602 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1769904000) TO (1772323200);
CREATE TABLE bitflyer_orderbook_202603 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1772323200) TO (1775001600);
CREATE TABLE bitflyer_orderbook_202604 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1775001600) TO (1777593600);
CREATE TABLE bitflyer_orderbook_202605 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1777593600) TO (1780272000);
CREATE TABLE bitflyer_orderbook_202606 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1780272000) TO (1782864000);
CREATE TABLE bitflyer_orderbook_202607 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1782864000) TO (1785542400);
CREATE TABLE bitflyer_orderbook_202608 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1785542400) TO (1788220800);
CREATE TABLE bitflyer_orderbook_202609 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1788220800) TO (1790812800);
CREATE TABLE bitflyer_orderbook_202610 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1790812800) TO (1793491200);
CREATE TABLE bitflyer_orderbook_202611 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1793491200) TO (1796083200);
CREATE TABLE bitflyer_orderbook_202612 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1796083200) TO (1798761600);
CREATE TABLE bitflyer_orderbook_202701 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1798761600) TO (1801440000);
CREATE TABLE bitflyer_orderbook_202702 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1801440000) TO (1803859200);
CREATE TABLE bitflyer_orderbook_202703 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1803859200) TO (1806537600);
CREATE TABLE bitflyer_orderbook_202704 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1806537600) TO (1809129600);
CREATE TABLE bitflyer_orderbook_202705 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1809129600) TO (1811808000);
CREATE TABLE bitflyer_orderbook_202706 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1811808000) TO (1814400000);
CREATE TABLE bitflyer_orderbook_202707 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1814400000) TO (1817078400);
CREATE TABLE bitflyer_orderbook_202708 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1817078400) TO (1819756800);
CREATE TABLE bitflyer_orderbook_202709 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1819756800) TO (1822348800);
CREATE TABLE bitflyer_orderbook_202710 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1822348800) TO (1825027200);
CREATE TABLE bitflyer_orderbook_202711 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1825027200) TO (1827619200);
CREATE TABLE bitflyer_orderbook_202712 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1827619200) TO (1830297600);
CREATE TABLE bitflyer_orderbook_202801 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1830297600) TO (1832976000);
CREATE TABLE bitflyer_orderbook_202802 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1832976000) TO (1835481600);
CREATE TABLE bitflyer_orderbook_202803 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1835481600) TO (1838160000);
CREATE TABLE bitflyer_orderbook_202804 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1838160000) TO (1840752000);
CREATE TABLE bitflyer_orderbook_202805 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1840752000) TO (1843430400);
CREATE TABLE bitflyer_orderbook_202806 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1843430400) TO (1846022400);
CREATE TABLE bitflyer_orderbook_202807 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1846022400) TO (1848700800);
CREATE TABLE bitflyer_orderbook_202808 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1848700800) TO (1851379200);
CREATE TABLE bitflyer_orderbook_202809 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1851379200) TO (1853971200);
CREATE TABLE bitflyer_orderbook_202810 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1853971200) TO (1856649600);
CREATE TABLE bitflyer_orderbook_202811 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1856649600) TO (1859241600);
CREATE TABLE bitflyer_orderbook_202812 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1859241600) TO (1861920000);
CREATE TABLE bitflyer_orderbook_202901 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1861920000) TO (1864598400);
CREATE TABLE bitflyer_orderbook_202902 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1864598400) TO (1867017600);
CREATE TABLE bitflyer_orderbook_202903 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1867017600) TO (1869696000);
CREATE TABLE bitflyer_orderbook_202904 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1869696000) TO (1872288000);
CREATE TABLE bitflyer_orderbook_202905 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1872288000) TO (1874966400);
CREATE TABLE bitflyer_orderbook_202906 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1874966400) TO (1877558400);
CREATE TABLE bitflyer_orderbook_202907 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1877558400) TO (1880236800);
CREATE TABLE bitflyer_orderbook_202908 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1880236800) TO (1882915200);
CREATE TABLE bitflyer_orderbook_202909 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1882915200) TO (1885507200);
CREATE TABLE bitflyer_orderbook_202910 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1885507200) TO (1888185600);
CREATE TABLE bitflyer_orderbook_202911 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1888185600) TO (1890777600);
CREATE TABLE bitflyer_orderbook_202912 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1890777600) TO (1893456000);
CREATE TABLE bitflyer_orderbook_203001 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1893456000) TO (1896134400);
CREATE TABLE bitflyer_orderbook_203002 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1896134400) TO (1898553600);
CREATE TABLE bitflyer_orderbook_203003 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1898553600) TO (1901232000);
CREATE TABLE bitflyer_orderbook_203004 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1901232000) TO (1903824000);
CREATE TABLE bitflyer_orderbook_203005 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1903824000) TO (1906502400);
CREATE TABLE bitflyer_orderbook_203006 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1906502400) TO (1909094400);
CREATE TABLE bitflyer_orderbook_203007 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1909094400) TO (1911772800);
CREATE TABLE bitflyer_orderbook_203008 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1911772800) TO (1914451200);
CREATE TABLE bitflyer_orderbook_203009 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1914451200) TO (1917043200);
CREATE TABLE bitflyer_orderbook_203010 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1917043200) TO (1919721600);
CREATE TABLE bitflyer_orderbook_203011 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1919721600) TO (1922313600);
CREATE TABLE bitflyer_orderbook_203012 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1922313600) TO (1924992000);
CREATE TABLE bitflyer_orderbook_203101 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1924992000) TO (1927670400);
CREATE TABLE bitflyer_orderbook_203102 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1927670400) TO (1930089600);
CREATE TABLE bitflyer_orderbook_203103 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1930089600) TO (1932768000);
CREATE TABLE bitflyer_orderbook_203104 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1932768000) TO (1935360000);
CREATE TABLE bitflyer_orderbook_203105 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1935360000) TO (1938038400);
CREATE TABLE bitflyer_orderbook_203106 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1938038400) TO (1940630400);
CREATE TABLE bitflyer_orderbook_203107 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1940630400) TO (1943308800);
CREATE TABLE bitflyer_orderbook_203108 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1943308800) TO (1945987200);
CREATE TABLE bitflyer_orderbook_203109 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1945987200) TO (1948579200);
CREATE TABLE bitflyer_orderbook_203110 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1948579200) TO (1951257600);
CREATE TABLE bitflyer_orderbook_203111 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1951257600) TO (1953849600);
CREATE TABLE bitflyer_orderbook_203112 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1953849600) TO (1956528000);
CREATE TABLE bitflyer_orderbook_203201 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1956528000) TO (1959206400);
CREATE TABLE bitflyer_orderbook_203202 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1959206400) TO (1961712000);
CREATE TABLE bitflyer_orderbook_203203 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1961712000) TO (1964390400);
CREATE TABLE bitflyer_orderbook_203204 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1964390400) TO (1966982400);
CREATE TABLE bitflyer_orderbook_203205 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1966982400) TO (1969660800);
CREATE TABLE bitflyer_orderbook_203206 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1969660800) TO (1972252800);
CREATE TABLE bitflyer_orderbook_203207 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1972252800) TO (1974931200);
CREATE TABLE bitflyer_orderbook_203208 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1974931200) TO (1977609600);
CREATE TABLE bitflyer_orderbook_203209 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1977609600) TO (1980201600);
CREATE TABLE bitflyer_orderbook_203210 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1980201600) TO (1982880000);
CREATE TABLE bitflyer_orderbook_203211 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1982880000) TO (1985472000);
CREATE TABLE bitflyer_orderbook_203212 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1985472000) TO (1988150400);
CREATE TABLE bitflyer_orderbook_203301 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1988150400) TO (1990828800);
CREATE TABLE bitflyer_orderbook_203302 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1990828800) TO (1993248000);
CREATE TABLE bitflyer_orderbook_203303 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1993248000) TO (1995926400);
CREATE TABLE bitflyer_orderbook_203304 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1995926400) TO (1998518400);
CREATE TABLE bitflyer_orderbook_203305 PARTITION OF bitflyer_orderbook FOR VALUES FROM (1998518400) TO (2001196800);
CREATE TABLE bitflyer_orderbook_203306 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2001196800) TO (2003788800);
CREATE TABLE bitflyer_orderbook_203307 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2003788800) TO (2006467200);
CREATE TABLE bitflyer_orderbook_203308 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2006467200) TO (2009145600);
CREATE TABLE bitflyer_orderbook_203309 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2009145600) TO (2011737600);
CREATE TABLE bitflyer_orderbook_203310 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2011737600) TO (2014416000);
CREATE TABLE bitflyer_orderbook_203311 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2014416000) TO (2017008000);
CREATE TABLE bitflyer_orderbook_203312 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2017008000) TO (2019686400);
CREATE TABLE bitflyer_orderbook_203401 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2019686400) TO (2022364800);
CREATE TABLE bitflyer_orderbook_203402 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2022364800) TO (2024784000);
CREATE TABLE bitflyer_orderbook_203403 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2024784000) TO (2027462400);
CREATE TABLE bitflyer_orderbook_203404 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2027462400) TO (2030054400);
CREATE TABLE bitflyer_orderbook_203405 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2030054400) TO (2032732800);
CREATE TABLE bitflyer_orderbook_203406 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2032732800) TO (2035324800);
CREATE TABLE bitflyer_orderbook_203407 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2035324800) TO (2038003200);
CREATE TABLE bitflyer_orderbook_203408 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2038003200) TO (2040681600);
CREATE TABLE bitflyer_orderbook_203409 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2040681600) TO (2043273600);
CREATE TABLE bitflyer_orderbook_203410 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2043273600) TO (2045952000);
CREATE TABLE bitflyer_orderbook_203411 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2045952000) TO (2048544000);
CREATE TABLE bitflyer_orderbook_203412 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2048544000) TO (2051222400);
CREATE TABLE bitflyer_orderbook_203501 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2051222400) TO (2053900800);
CREATE TABLE bitflyer_orderbook_203502 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2053900800) TO (2056320000);
CREATE TABLE bitflyer_orderbook_203503 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2056320000) TO (2058998400);
CREATE TABLE bitflyer_orderbook_203504 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2058998400) TO (2061590400);
CREATE TABLE bitflyer_orderbook_203505 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2061590400) TO (2064268800);
CREATE TABLE bitflyer_orderbook_203506 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2064268800) TO (2066860800);
CREATE TABLE bitflyer_orderbook_203507 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2066860800) TO (2069539200);
CREATE TABLE bitflyer_orderbook_203508 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2069539200) TO (2072217600);
CREATE TABLE bitflyer_orderbook_203509 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2072217600) TO (2074809600);
CREATE TABLE bitflyer_orderbook_203510 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2074809600) TO (2077488000);
CREATE TABLE bitflyer_orderbook_203511 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2077488000) TO (2080080000);
CREATE TABLE bitflyer_orderbook_203512 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2080080000) TO (2082758400);
CREATE TABLE bitflyer_orderbook_203601 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2082758400) TO (2085436800);
CREATE TABLE bitflyer_orderbook_203602 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2085436800) TO (2087942400);
CREATE TABLE bitflyer_orderbook_203603 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2087942400) TO (2090620800);
CREATE TABLE bitflyer_orderbook_203604 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2090620800) TO (2093212800);
CREATE TABLE bitflyer_orderbook_203605 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2093212800) TO (2095891200);
CREATE TABLE bitflyer_orderbook_203606 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2095891200) TO (2098483200);
CREATE TABLE bitflyer_orderbook_203607 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2098483200) TO (2101161600);
CREATE TABLE bitflyer_orderbook_203608 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2101161600) TO (2103840000);
CREATE TABLE bitflyer_orderbook_203609 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2103840000) TO (2106432000);
CREATE TABLE bitflyer_orderbook_203610 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2106432000) TO (2109110400);
CREATE TABLE bitflyer_orderbook_203611 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2109110400) TO (2111702400);
CREATE TABLE bitflyer_orderbook_203612 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2111702400) TO (2114380800);
CREATE TABLE bitflyer_orderbook_203701 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2114380800) TO (2117059200);
CREATE TABLE bitflyer_orderbook_203702 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2117059200) TO (2119478400);
CREATE TABLE bitflyer_orderbook_203703 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2119478400) TO (2122156800);
CREATE TABLE bitflyer_orderbook_203704 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2122156800) TO (2124748800);
CREATE TABLE bitflyer_orderbook_203705 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2124748800) TO (2127427200);
CREATE TABLE bitflyer_orderbook_203706 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2127427200) TO (2130019200);
CREATE TABLE bitflyer_orderbook_203707 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2130019200) TO (2132697600);
CREATE TABLE bitflyer_orderbook_203708 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2132697600) TO (2135376000);
CREATE TABLE bitflyer_orderbook_203709 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2135376000) TO (2137968000);
CREATE TABLE bitflyer_orderbook_203710 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2137968000) TO (2140646400);
CREATE TABLE bitflyer_orderbook_203711 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2140646400) TO (2143238400);
CREATE TABLE bitflyer_orderbook_203712 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2143238400) TO (2145916800);
CREATE TABLE bitflyer_orderbook_203801 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2145916800) TO (2148595200);
CREATE TABLE bitflyer_orderbook_203802 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2148595200) TO (2151014400);
CREATE TABLE bitflyer_orderbook_203803 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2151014400) TO (2153692800);
CREATE TABLE bitflyer_orderbook_203804 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2153692800) TO (2156284800);
CREATE TABLE bitflyer_orderbook_203805 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2156284800) TO (2158963200);
CREATE TABLE bitflyer_orderbook_203806 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2158963200) TO (2161555200);
CREATE TABLE bitflyer_orderbook_203807 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2161555200) TO (2164233600);
CREATE TABLE bitflyer_orderbook_203808 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2164233600) TO (2166912000);
CREATE TABLE bitflyer_orderbook_203809 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2166912000) TO (2169504000);
CREATE TABLE bitflyer_orderbook_203810 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2169504000) TO (2172182400);
CREATE TABLE bitflyer_orderbook_203811 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2172182400) TO (2174774400);
CREATE TABLE bitflyer_orderbook_203812 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2174774400) TO (2177452800);
CREATE TABLE bitflyer_orderbook_203901 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2177452800) TO (2180131200);
CREATE TABLE bitflyer_orderbook_203902 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2180131200) TO (2182550400);
CREATE TABLE bitflyer_orderbook_203903 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2182550400) TO (2185228800);
CREATE TABLE bitflyer_orderbook_203904 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2185228800) TO (2187820800);
CREATE TABLE bitflyer_orderbook_203905 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2187820800) TO (2190499200);
CREATE TABLE bitflyer_orderbook_203906 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2190499200) TO (2193091200);
CREATE TABLE bitflyer_orderbook_203907 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2193091200) TO (2195769600);
CREATE TABLE bitflyer_orderbook_203908 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2195769600) TO (2198448000);
CREATE TABLE bitflyer_orderbook_203909 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2198448000) TO (2201040000);
CREATE TABLE bitflyer_orderbook_203910 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2201040000) TO (2203718400);
CREATE TABLE bitflyer_orderbook_203911 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2203718400) TO (2206310400);
CREATE TABLE bitflyer_orderbook_203912 PARTITION OF bitflyer_orderbook FOR VALUES FROM (2206310400) TO (2208988800);


--
-- Name: bitflyer_ticker; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitflyer_ticker (
    symbol smallint NOT NULL,
    tick_id bigint NOT NULL,
    state smallint,
    unixtime bigint,
    bid real,
    ask real,
    bid_size real,
    ask_size real,
    total_bid_depth real,
    total_ask_depth real,
    market_bid_size real,
    market_ask_size real,
    last_traded_price real,
    volume real,
    volume_by_product real
);


ALTER TABLE public.bitflyer_ticker OWNER TO postgres;

--
-- Name: bitflyer_executions bitflyer_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitflyer_executions
    ADD CONSTRAINT bitflyer_executions_pkey PRIMARY KEY (symbol, id);


--
-- Name: bitflyer_fundingrate bitflyer_fundingrate_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitflyer_fundingrate
    ADD CONSTRAINT bitflyer_fundingrate_pkey PRIMARY KEY (symbol, unixtime);


--
-- Name: bitflyer_ticker bitflyer_ticker_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitflyer_ticker
    ADD CONSTRAINT bitflyer_ticker_pkey PRIMARY KEY (symbol, tick_id);

--
-- Name: bitflyer_executions_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_0 ON public.bitflyer_executions USING btree (symbol);


--
-- Name: bitflyer_executions_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_1 ON public.bitflyer_executions USING btree (id);


--
-- Name: bitflyer_executions_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_executions_2 ON public.bitflyer_executions USING btree (unixtime);


--
-- Name: bitflyer_orderbook_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_orderbook_0 ON public.bitflyer_orderbook USING btree (symbol);


--
-- Name: bitflyer_orderbook_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_orderbook_1 ON public.bitflyer_orderbook USING btree (unixtime);


--
-- Name: bitflyer_ticker_0; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_0 ON public.bitflyer_ticker USING btree (symbol);


--
-- Name: bitflyer_ticker_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_1 ON public.bitflyer_ticker USING btree (tick_id);


--
-- Name: bitflyer_ticker_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX bitflyer_ticker_2 ON public.bitflyer_ticker USING btree (unixtime);
