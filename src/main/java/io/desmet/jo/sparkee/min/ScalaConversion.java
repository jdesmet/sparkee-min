package io.desmet.jo.sparkee.min;

//import com.att.nqp.db.ConnectionSupplier;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Function0;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/**
 *
 * @author jdesmet
 */
public class ScalaConversion {
  public static final scala.collection.immutable.List NIL = scala.collection.immutable.Nil$.MODULE$;

  // Wrapper for Java Functions
  public static <A, B> Function1<A, B> function(java.util.function.Function<A, B> f) {
    return new AbstractFunction1<A, B>() {
      @Override
      public B apply(A t1) {
        return f.apply(t1);
      }
    };
  }

  // Wrapper for Java Functions
  public static <A> Function1<A, Integer> functionx(java.util.function.Consumer<A> f) {
    return new AbstractFunction1<A, Integer>() {
      @Override
      public Integer apply(A t1) {
        f.accept(t1);
        return (0);
      }
    };
  }

  @FunctionalInterface
  public interface SerializableConsumer<A> extends java.util.function.Consumer<A> , Serializable { }
  static private abstract class SerializableAbstractFunction1<A,B> extends AbstractFunction1<A, B> implements Serializable {}
  // Wrapper for Java Functions
  public static <A> Function1<A, BoxedUnit> function(SerializableConsumer<A> f) {
    return new SerializableAbstractFunction1<A, BoxedUnit>() {
      @Override
      public BoxedUnit apply(A t1) {
        f.accept(t1);
        return BoxedUnit.UNIT;
      }
    };
  }

  // Wrapper for Java Functions
  public static <K, V> Function1<Tuple2<K, V>, Integer> function(java.util.function.BiConsumer<K, V> f) {
    return new AbstractFunction1<Tuple2<K, V>, Integer>() {
      @Override
      public Integer apply(Tuple2<K, V> t) {
        f.accept(t._1, t._2);
        return (0);
      }
    };
  }

  // Wrapper for Java Functions
  public static <A> Function0<A> function(java.util.function.Supplier<A> f) {
    return new AbstractFunction0<A>() {
      @Override
      public A apply() {
        return f.get();
      }
    };
  }

  // Wrapper for Java Functions
  public static <A> Function1<A,Boolean> function(java.util.function.Predicate<A> f) {
    return new AbstractFunction1<A,Boolean>() {
      @Override
      public Boolean apply(A t) {
        return f.test(t);
      }
    };
  }

  /*public static final class ScalaConnectionSupplierWrapper extends AbstractFunction0<Connection> implements Serializable {
    final private ConnectionSupplier f;
    ScalaConnectionSupplierWrapper(ConnectionSupplier f) {
      this.f = f;
    }
    
    @Override
    public Connection apply() {
      try {
        return f.getConnection();
      } catch (SQLException ex) {
        throw new Error(ex);
      }
    }
  }
  
  // Wrapper
  public static Function0<Connection> function(ConnectionSupplier f) {
    return new ScalaConnectionSupplierWrapper(f);
  }*/

  public static <T> scala.reflect.ClassTag<T> classTag(Class<?> clazz) {
    return scala.reflect.ClassTag$.MODULE$.apply(clazz);
  }
  
  public static <T> scala.collection.Iterator<T> iterator(java.util.Iterator<T> iterator) {
    return scala.collection.JavaConversions.asScalaIterator(iterator);
  }
  
  public static <T> JavaRDD<T> rdd(RDD<T> rdd, Class<T> clazz) {
    return JavaRDD.fromRDD(rdd, classTag(clazz));
  }
}
