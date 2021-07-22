public enum DataColumn {
    STATION(0),
    DATE(1),
    LATITUDE(2),
    LONGITUDE(3),
    ELEVATION(4),
    NAME(5),
    TEMP(6),
    TEMP_ATTRIBUTES(7),
    DEWP(8),
    DEWP_ATTRIBUTES(9),
    SLP(10),
    SLP_ATTRIBUTES(11),
    STP(12),
    STP_ATTRIBUTES(13),
    VISIB(14),
    VISIB_ATTRIBUTES(15),
    WDSP(16),
    WDSP_ATTRIBUTES(17),
    MXSPD(18),
    GUST(19),
    MAX(20),
    MAX_ATTRIBUTES(21),
    MIN(22),
    MIN_ATTRIBUTES(23),
    PRCP(24),
    PRCP_ATTRIBUTES(25),
    SNDP(26),
    FRSHTT(27);

    public final int index;

    private DataColumn(int index) {
        this.index =  index;
    }
}
