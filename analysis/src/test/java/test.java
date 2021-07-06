import com.wei.util.DateUtils;
import org.junit.jupiter.api.Test;


public class test {

    @Test
    public void test1() {
        Long starttime=1623841990204L;
        Long endTime=1623841990204L;
        System.out.println(DateUtils.getStartOfTheDay(starttime));
        System.out.println(DateUtils.getEndOfTheDay(endTime));
        System.out.println(DateUtils.getBeforeOfTheTime(starttime));
        System.out.println(DateUtils.getBehindOfTheTime(endTime));
    }
}



