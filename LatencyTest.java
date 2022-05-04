import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;


/**
 * @deprecated
 * This is a program that automatically retrives latency test results from egress.
 * Also, this program reads from the old egress, which is not used anymore.
 */
public class LatencyTest {


  /**
   * Get the egress output corresponding with latency test
   * @param url
   * @return result: the response of the get request
   * @throws Exception
   */
  public static String get(String url) throws Exception {
    URL serverUrl = new URL(url);
    HttpURLConnection con = (HttpURLConnection) serverUrl.openConnection();
    con.setRequestMethod("GET");
    int status = con.getResponseCode();
    String result = "";
    if (status == 200) {
      BufferedReader in = new BufferedReader(
          new InputStreamReader(con.getInputStream())
      );
      String inputLine;
      StringBuffer content = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
        content.append(inputLine);
      }
      in.close();
      result = content.toString();
    }
    // System.out.println(result);
    return result;
  }

  public static void main(String[] args) throws Exception {
    String filename = "latencyTest.txt";
    FileWriter filewriter = new FileWriter(filename);
    PrintWriter printWriter = new PrintWriter(filewriter);
    String url = "http://localhost:8091/add-edge-latency";

    long start = System.currentTimeMillis();
    long maxPeriod = 20 * 1000 * 60;
    String egressOutput;
    while (System.currentTimeMillis() - start < maxPeriod) {
      egressOutput = get(url);
      String[] parts = egressOutput.split(",");
      // System.out.println(Arrays.toString(parts));
      // we only need to record latency of one vertex as a representative
      if (parts[0].equals("1")) {
        printWriter.println(egressOutput);
      }
      // pull from egress every 100 milliseconds
      Thread.sleep(5);
    }
    // close writer
    printWriter.close();
  }
}
