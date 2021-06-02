import com.wei.util.ConfigUtil;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    public void test2() throws ParseException {
        Date date = new Date();
        //System.out.println(date);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = dateFormat.format(date);
        Date parse = dateFormat.parse(format);
        //System.out.println(parse);
        if (test3().equals("")) {
            System.out.println(1111);
        } else {
            System.out.println(2222);
        }
    }
    private String test3(){
        return "";
    }
}



