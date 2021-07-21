import java.io.Serializable;

public class DataRow implements Serializable {
    private String station;
    private String date;
    private Double latitude;
    private Double longitude;
    private Double elevation;
    private String name;
    private Double temp;
    private String tempAttributes;
    private Double dewp;
    private String dewpAttributes;
    private Double slp;
    private String splAttributes;
    private Double stp;
    private String stpAttributes;
    private Double visib;
    private String visibAttributes;
    private Double wdsp;
    private String wdspAttributes;
    private Double mxspd;
    private Double gust;
    private Double max;
    private String maxAttributes;
    private Double min;
    private String minAttributes;
    private Double prcp;
    private String prcpAttributes;
    private Double sndp;
    private String frshtt;


    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getElevation() {
        return elevation;
    }

    public void setElevation(Double elevation) {
        this.elevation = elevation;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    public String getTempAttributes() {
        return tempAttributes;
    }

    public void setTempAttributes(String tempAttributes) {
        this.tempAttributes = tempAttributes;
    }

    public Double getDewp() {
        return dewp;
    }

    public void setDewp(Double dewp) {
        this.dewp = dewp;
    }

    public String getDewpAttributes() {
        return dewpAttributes;
    }

    public void setDewpAttributes(String dewpAttributes) {
        this.dewpAttributes = dewpAttributes;
    }

    public Double getSlp() {
        return slp;
    }

    public void setSlp(Double slp) {
        this.slp = slp;
    }

    public String getSplAttributes() {
        return splAttributes;
    }

    public void setSplAttributes(String splAttributes) {
        this.splAttributes = splAttributes;
    }

    public Double getStp() {
        return stp;
    }

    public void setStp(Double stp) {
        this.stp = stp;
    }

    public String getStpAttributes() {
        return stpAttributes;
    }

    public void setStpAttributes(String stpAttributes) {
        this.stpAttributes = stpAttributes;
    }

    public Double getVisib() {
        return visib;
    }

    public void setVisib(Double visib) {
        this.visib = visib;
    }

    public String getVisibAttributes() {
        return visibAttributes;
    }

    public void setVisibAttributes(String visibAttributes) {
        this.visibAttributes = visibAttributes;
    }

    public Double getWdsp() {
        return wdsp;
    }

    public void setWdsp(Double wdsp) {
        this.wdsp = wdsp;
    }

    public String getWdspAttributes() {
        return wdspAttributes;
    }

    public void setWdspAttributes(String wdspAttributes) {
        this.wdspAttributes = wdspAttributes;
    }

    public Double getMxspd() {
        return mxspd;
    }

    public void setMxspd(Double mxspd) {
        this.mxspd = mxspd;
    }

    public Double getGust() {
        return gust;
    }

    public void setGust(Double gust) {
        this.gust = gust;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public String getMaxAttributes() {
        return maxAttributes;
    }

    public void setMaxAttributes(String maxAttributes) {
        this.maxAttributes = maxAttributes;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public String getMinAttributes() {
        return minAttributes;
    }

    public void setMinAttributes(String minAttributes) {
        this.minAttributes = minAttributes;
    }

    public Double getPrcp() {
        return prcp;
    }

    public void setPrcp(Double prcp) {
        this.prcp = prcp;
    }

    public String getPrcpAttributes() {
        return prcpAttributes;
    }

    public void setPrcpAttributes(String prcpAttributes) {
        this.prcpAttributes = prcpAttributes;
    }

    public Double getSndp() {
        return sndp;
    }

    public void setSndp(Double sndp) {
        this.sndp = sndp;
    }

    public String getFrshtt() {
        return frshtt;
    }

    public void setFrshtt(String frshtt) {
        this.frshtt = frshtt;
    }
}
