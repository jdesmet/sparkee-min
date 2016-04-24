package io.desmet.jo.sparkee.min;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This is the BaseDriver program which serves as the parent class for all driver
 * Programs. It is developed in such a way that it handles all default stuff for a
 * project. This includes 1./ reading properties from the packaged file (in src/main/resources),
 * 2./ reading and overriding with a file defined in ~/.spark/default.properties. 
 * The benefit of this approach is that it does not require any classpath to be set for
 * the properties to be loaded. The dynamic typing provides additional typesafety
 * and enforces correct implementation of the constructor. More specifically, it requires
 * the class to be extended as follows:
 * <pre>
    public class RedialProgram extends RedialBaseDriver<RedialProgram> {

      public RedialProgram() {
        super(RedialProgram.class);
      }
    ...
 * </pre>
 * @author jdesmet
 */
public abstract class BaseDriver<T extends BaseDriver> {
  static private final Logger BASE_LOGGER = Logger.getLogger(BaseDriver.class.getName());
  // XXX: Do NOT make it static! I know naming convention is wrong, and that
  // code checkers would suggest to make it so. However, this is a driver program
  // with a very limitted life cycle. Leave it. (jdesmet)
  protected final Logger LOGGER;
  
  static protected final Path HOME = Paths.get(System.getProperty("user.home"));
  static protected final Path HOME_PROPERTIES_DIR = HOME.resolve(".spark");
  static protected final Path DEFAULT_PROPERTIES_FILE = HOME_PROPERTIES_DIR.resolve("default.properties");
  
  /**
   * Contains BASE configuration data. The Base properties are loaded from 2 sources, where in
   * below order, each next source overwrites the previous. 
   * <ul>
   * <li>From the classpath as /default.properties. This file is usually contained
   * within the package or jar file deployed.</li>
   * <li>From ~/.spark/default.properties. This file is intended as a common
   * configuration between multiple modules.</li>
   * </ul>
   */
  static final private Properties BASE_PROPERTIES = new Properties();
  
  
  /**
   * Contains configuration data. The properties are loaded from 3 sources, where in
   * below order, each next source overwrites the previous. This is implemented as a Map
   * to make it more lightweight. Note that this is not Thread safe, so use it only
   * for reading configuration, never for setting (unless when done from within the driver
   * thread before any spark tasks are spawned).
   * <ul>
   * <li>From the classpath as /default.properties. This file is usually contained
   * within the package or jar file deployed.</li>
   * <li>From ~/.spark/default.properties. This file is intended as a common
   * configuration between multiple modules.</li>
   * <li>From ~/.spark/AppName.properties. The file name will match the Class Name of the
   * Driver Program. If two Drivers with the same name are contained within different package names,
   * then obviously we would have a conflict. Avoid this conflict.</li>
   * </ul>
   * 
   */
  final protected Map<String,String> PROPERTIES;
  
  // Boilerplate Code to read configuration.
  static {
    try (InputStream is = BaseDriver.class.getResourceAsStream("/default.properties")) {
      // Implementation of Properties.load does not gain benefit from a BufferedInputStream.
      if (is != null) {
        BASE_PROPERTIES.load(is);
        BASE_LOGGER.log(Level.INFO, "Loaded /default.properties as a Resource");
      } else {
        BASE_LOGGER.log(Level.WARNING, "Could not load /default.properties as a Resource (src/main/resources); trying to ignore.");
      }
    } catch (IOException ex) {
      BASE_LOGGER.log(Level.WARNING, "Could not load /default.properties as a Resource (src/main/resources); trying to ignore; {0}", ex.getMessage());
    }
    try (InputStream is = Files.newInputStream(DEFAULT_PROPERTIES_FILE)) {
      // Implementation of Properties.load does not gain benefit from a BufferedInputStream.
      BASE_PROPERTIES.load(is);
      BASE_LOGGER.log(Level.INFO, "Loaded \"{0}\"",new String [] { DEFAULT_PROPERTIES_FILE.toString()});
    } catch (IOException ex) {
      BASE_LOGGER.log(Level.WARNING, "Could not load \"{0}\" ({1}); trying to ignore ...", new String [] { DEFAULT_PROPERTIES_FILE.toString(), ex.getMessage()});
    }
  }
  
  public String getInputPath(String fileName) {
    String inputDir = PROPERTIES.get("data.input.path");
    System.out.println(inputDir);
    String fullInputPath = Paths.get(inputDir).resolve(fileName).toString();
    System.out.println(fullInputPath);
    return fullInputPath;
  }
  
  //public ConnectionSpecification getConnectionSpecification(String propertyName) {
  //  String url = PROPERTIES.get(propertyName+".url");
  //  String username = PROPERTIES.get(propertyName+".username");
  //  String password = PROPERTIES.get(propertyName+".password");
  //  // Throw unchecked exceptions to have some meaningful error state.
  //  if (url==null) throw new IllegalStateException("\""+propertyName+".url\" not defined for in default.properties");
  //  if (username==null) throw new IllegalStateException("\""+propertyName+".username\" not defined for in default.properties");
  //  if (password==null) throw new IllegalStateException("\""+propertyName+".password\" not defined for in default.properties");
  //  return new ConnectionFromUrl(url, username, password);  
  //}
  
  protected SparkConf CONF;
  protected SparkContext SPARK_CONTEXT;
  protected JavaSparkContext JAVA_SPARK_CONTEXT;

  protected BaseDriver(Class<T> clazz) {
    this(clazz,null);
  }
  
  /**
   * Register the Application's Driver class. This is required for custom logging,
   * registering the application name with Spark, and for making sure custom application
   * configurations are loaded if available.
   * @param clazz 
   */
  protected BaseDriver(Class<T> clazz,InputStream in) {
    LOGGER = Logger.getLogger(clazz.getName()); // Need name with classpath
    final String appName = clazz.getSimpleName();
    
    // Load Application specific properties in a tempory properties map
    final Path propertiesFile = HOME_PROPERTIES_DIR.resolve(appName+".properties");
    final Properties properties = new Properties();
    try (InputStream is = Files.newInputStream(propertiesFile)) {
      // Implementation of Properties.load does not gain benefit from a BufferedInputStream.
      properties.load(is);
      LOGGER.log(Level.INFO, "Loaded \"{0}\"",new String [] { propertiesFile.toString()});
    } catch (IOException ex) {
      LOGGER.log(Level.WARNING, "Could not load \"{0}\" ({1}); trying to ignore ...", new String [] { propertiesFile.toString(), ex.getMessage()});
    }
    
    PROPERTIES = new HashMap<>(BASE_PROPERTIES.size());
    
    // Copy BASE_PROPERTIES over in the PROPERTIES Map
    BASE_PROPERTIES.forEach((k,v)->PROPERTIES.put((String)k, (String)v));
    
    // Copy Application Properties over in the PROPERTIES Map (overwriting as encountered).
    properties.forEach((k,v)->PROPERTIES.put((String)k, (String)v));
    
    // Copy command-line defines (-Dxxxx=yyy) over in the PROPERTIES Map (overwriting as encountered).
    System.getProperties().forEach((k,v)->PROPERTIES.put((String)k, (String)v));
    
    // Override from in
    if (in != null) {
      Properties p = new Properties();
      try {
        p.load(in);
        p.forEach((k,v)->PROPERTIES.put((String)k, (String)v));
      } catch (IOException ex) {
        LOGGER.log(Level.WARNING, "Could not load properties from Input ...", new String [] { propertiesFile.toString(), ex.getMessage()});
      }
    }

    // Boilerplate Spark Initialization
    CONF = new SparkConf().setAppName(appName);
    if (!CONF.getOption("spark.master").isDefined()) {
      // If no Spark master set, then assume this will be a local run
      // This will even work for remote execution.
      CONF.setMaster(PROPERTIES.getOrDefault("spark.master.default", "local[5]"));
    }
    JAVA_SPARK_CONTEXT = new JavaSparkContext(CONF);
    SPARK_CONTEXT = JAVA_SPARK_CONTEXT.sc();
  }
  
  // I am itching to implement Runnable, but that does not allow for checked
  // exceptions. So not doing it, as I wont run it as a part of the base Java
  // Thread/Executor framework anyhow.
  abstract public void run() throws Exception;
}
