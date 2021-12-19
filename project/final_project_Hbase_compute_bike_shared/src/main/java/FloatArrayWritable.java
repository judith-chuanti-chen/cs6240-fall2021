import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.List;

public class FloatArrayWritable extends ArrayWritable{
  public FloatArrayWritable() {
    super(FloatWritable.class);
  }

  public FloatArrayWritable(List<Float> floats) {
    super(FloatWritable.class);
    FloatWritable[] floatWritables = new FloatWritable[floats.size()];
    for (int i = 0; i < floats.size(); i++) {
      floatWritables[i] = new FloatWritable(floats.get(i));
    }
    set(floatWritables);
  }
}
