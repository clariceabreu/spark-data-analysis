public enum DatasetColumn {
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
    STP_ATTRIBUTES(12),
    VISIB(13),
    VISIB_ATTRIBUTES(14),
    WDSP(15),
    WDSP_ATTRIBUTES(16),
    MXSPD(17),
    GUST(18),
    MAX(19),
    MAX_ATTRIBUTES(20),
    MIN(21),
    MIN_ATTRIBUTES(22),
    PRCP(23),
    PRCP_ATTRIBUTES(24),
    SNDP(25),
    FRSHTT(26);

    public final int index;

    private DatasetColumn(int index) {
        this.index =  index;
    }
}
