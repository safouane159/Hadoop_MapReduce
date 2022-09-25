
package mbds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

class GraphNodeWritable implements Writable {

	public int depth = 0;
	public String color = "";
	public ArrayList<String> childs = new ArrayList<>();

	public GraphNodeWritable() {};

	public GraphNodeWritable(String string) {
		String[] elems = string.split("\\|");
		depth = Integer.parseInt(elems[2]);
		color = elems[1];
		childs = new ArrayList<String>(Arrays.asList(elems[0].split(",")));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(get_serialized());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String line = in.readUTF();
		System.out.println("READING LINE" + line);
		String[] elems = line.split("\\|");
		depth = Integer.parseInt(elems[2]);
		System.out.println(depth + " Depth!");
		color = elems[1];
		childs = new ArrayList<String>(Arrays.asList(elems[0].split(",")));
	}

	public String get_serialized() {
		Iterator<String> iter = childs.iterator();
		String childsString = "";
		while (iter.hasNext()) {
			if (!childsString.equals("")) {
				childsString += ",";
			}
			childsString += iter.next();
		}
		return childsString + "|" + color + "|" + depth;
	} 
};
