import com.wei.kafka.ProduceDataKafka;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class DataTest {
   @Test
   public void test(){
       String behaviorString="pv,uv";
       Random random=new Random();
       String[] behaviorList = behaviorString.split(",");
       //随机userId
       for (int i = 0; i < 100; i++) {
           long userId=(long) Math.floor((random.nextDouble()*100000.0));
           //随机itemId
           long itemId=(long) Math.floor((random.nextDouble()*100000.0));
           //随机categoryId
           long categoryId=(long) Math.floor((random.nextDouble()*100000.0));
           //随机behavior
           String behavior = behaviorList[random.nextInt(2)];
           System.out.println(userId);
           System.out.println(itemId);
           System.out.println(categoryId);
           System.out.println(behavior);
       }
   }
}
