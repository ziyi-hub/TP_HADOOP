import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringAndInt2 implements WritableComparable<StringAndInt2> {

	private String tag;
	private Integer occurrences;
	
	public StringAndInt2() {
        // Default constructor is required for deserialization
    }
	
    // Constructeur sans arguments appelant super
    public StringAndInt2(String tag, Integer occ) {
        this.tag = tag;
		this.occurrences = occ;
    }
    
    // Getter pour le tag
    public String getTag() {
        return tag;
    }

    // Getter pour le nombre d'occurrences
    public Integer getOccurrences() {
        return occurrences;
    }

    @Override
	public int compareTo(StringAndInt2 other) {
		return Integer.compare(other.occurrences, this.occurrences);
	}

    @Override
	public void readFields(DataInput arg0) throws IOException {
		tag = arg0.readUTF();
        occurrences = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(tag);
		arg0.writeInt(occurrences);
	}
}
