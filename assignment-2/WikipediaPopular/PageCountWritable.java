import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;

/**
 * A Writable that represents a pair of long values.
 */
public class PageCountWritable implements Writable {

	private String a;
	private long b;

	private LongWritable writ = new LongWritable();
	private Text text = new Text();

	public PageCountWritable(String a, long b) {
		this.a = a;
		this.b = b;
	}

	public PageCountWritable() {
		this("", 0);
	}
	
	public String get_0() {
		return a;
	}
	public long get_1() {
		return b;
	}
	public void set(String a, long b) {
		this.a = a;
		this.b = b;		
	}

	public void write(DataOutput out) throws IOException {
		text.set(a);
		text.write(out);

		writ.set(b);
		writ.write(out);
	}
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
		a = text.toString();

		writ.readFields(in);
		b = writ.get();
	}
	
	public String toString() {
		return "(" + a + "," + Long.toString(b) + ")";
	}

}
