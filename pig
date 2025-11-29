import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class RemoveInvalidData extends EvalFunc<Boolean> {
@Override
public Boolean exec(Tuple input) throws IOException {
if (input == null || input.size() == 0)
return false;

try {
String name = (String) input.get(0);
int age = (Integer) input.get(1);

// Remove unwanted data: null/empty names or negative age
if (name == null || name.trim().isEmpty() || age < 0)
return false;
else
return true;

} catch (Exception e) {
throw new IOException("Error processing tuple", e);
}
}
}

javac -cp "C:\pig\pig-0.17.0.jar" RemoveInvalidData.java

jar -cf removeinvaliddata.jar RemoveInvalidData.class

Use the UDF in Pig Script

Create a Pig script file named filter_data.pig:

REGISTER 'removeinvaliddata.jar';

DEFINE RemoveInvalidData RemoveInvalidData();

data = LOAD 'input.csv' USING PigStorage(',') AS (name:chararray, age:int);

filtered = FILTER data BY RemoveInvalidData(name, age);

DUMP filtered;


//Run the Pig Script

Run the script in local mode:

pig -x local filter_data.pig



Input (input.csv):

John,25
,30
Sara,-5
Ali,40


Output (after filtering):

John,25
Ali,40
