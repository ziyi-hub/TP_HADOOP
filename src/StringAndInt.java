import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {
	
	private String tag;
	private Integer occurrences;
	
	public StringAndInt() {
    }
	
	StringAndInt(String tag, Integer occ){
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
	public int compareTo(StringAndInt other) {
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
