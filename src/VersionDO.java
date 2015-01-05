
public class VersionDO implements Comparable<VersionDO> {

    /** 值越小优先级越高 */
    private Integer index;

    @Override
    public int compareTo(VersionDO o) {
        // 优先级高的排前面，所以值小的排前面
        return o.index.compareTo(index);
    }

    private String version;

    private String voption;

    private String value;

    /**
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * @param version the version to set
     */
    public void setVersion(String version) {
        this.version = version.trim();
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    /**
     * @return the voption
     */
    public String getVoption() {
        return voption;
    }

    /**
     * @param voption the voption to set
     */
    public void setVoption(String voption) {
        this.voption = voption.trim();
    }

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(String value) {
        this.value = value;
    }

}
