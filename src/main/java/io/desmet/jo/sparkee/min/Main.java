package io.desmet.jo.sparkee.min;

import java.io.IOException;

/**
 *
 * @author jdesmet
 */
public class Main extends BaseDriver {
  public Main() throws IOException {
    super(Main.class);
  }

  @Override
  public void run() throws Exception {
  }
  
  public static void main(String [] args) throws IOException, Exception {
    new Main().run();
  }
}
