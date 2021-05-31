import com.wei.util.ConfigUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class test {
    private final Logger logger = LoggerFactory.getLogger(test.class);

    @Test
    public void test1(){
        String s="true1";
        try {
            if (!s.equals("true")){
                throw new Exception("不等于true");
            }
            System.out.println("还好执行吗");
        }catch (Exception e){
            System.out.println("出错了"+e);
            logger.error("2222");
        }

        System.out.println("1111");
    }
    @Test
    public void test2(){
        Byte n=(byte)0;

        System.out.println(Boolean.parseBoolean(n.toString()));
    }
}



