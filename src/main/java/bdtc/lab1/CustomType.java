package bdtc.lab1;
import com.google.common.collect.ComparisonChain;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor

public class CustomType implements WritableComparable<CustomType> {

    /**
     * Metric name representation
     */
    @Setter
    @Getter
    private String deviceName;
    /**
     * Metric timestamp
     */
    @Setter
    @Getter
    private long timestamp;
    /**
     * Scale value representation
     */
    @Setter
    @Getter
    private String scaleText;

    /**
     * Override of readFields() method
     * @param in output data to write
     * @throws IOException when IO operation fails
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        deviceName = in.readUTF();
        timestamp = in.readLong();
        scaleText = in.readUTF();
    }

    /**
     * Override of write() method
     * @param out output data to write
     * @throws IOException when IO operation fails
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(deviceName);
        out.writeLong(timestamp);
        out.writeUTF(scaleText);
    }

    /**
     * Override of compareTo() method
     */
    @Override
    public int compareTo(CustomType o) {
        return ComparisonChain.start().compare(deviceName, o.deviceName)
                .compare(timestamp, o.timestamp).compare(scaleText, o.scaleText).result();
    }

    /**
     * Override of equals() method
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomType that = (CustomType) o;
        return Objects.equals(deviceName,that.deviceName) && timestamp == that.timestamp && Objects.equals(scaleText, that.scaleText);
    }

    /**
     * Override of hashCode() method
     */
    @Override
    public int hashCode() {
        return Objects.hash(deviceName, timestamp, scaleText);
    }

    /**
     * Override of toString() method
     */
//    @Override
//    public String toString() {
//        return "metricIdWritable{" +
//                "metricId=" + deviceName +
//                ", timestamp=" + timestamp +
//                ", scaleText='" + scaleText + '\'' +
//                '}';
//    }
    @Override
    public String toString() {
        return
                deviceName +','+ timestamp +','+ scaleText + ",";
    }
}
