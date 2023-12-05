
public class StringAndInt implements Comparable<StringAndInt> {
	
	private String tag;
	private Integer occurrences;
	
	StringAndInt(String tag, Integer occ){
		this.tag = tag;
		this.occurrences = occ;
	}
	
	// Getter pour le tag
    public String getTag() {
        return tag;
    }

    // Getter pour le nombre d'occurrences
    public int getOccurrences() {
        return occurrences;
    }

	@Override
	public int compareTo(StringAndInt other) {
		// TODO Auto-generated method stub
		return Integer.compare(this.occurrences, other.occurrences);
	}
	
}
