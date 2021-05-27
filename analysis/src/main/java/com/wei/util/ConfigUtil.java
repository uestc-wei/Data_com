package com.wei.util;



import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@NoArgsConstructor
@AllArgsConstructor
@Data
public class ConfigUtil {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtil.class);
    /**
     * 配置
     */
    private Map<String,String> configureInfo=new HashMap<>();

    /**
     * 初始化配置类
     */
    public void init(){
        log.info("开始初始化配置类.........");
        Yaml yaml=new Yaml();
        try{
            InputStream inputStream = ConfigUtil.class.getClassLoader().getResourceAsStream("configure.yml");
            configureInfo = yaml.loadAs(inputStream, Map.class);
        }catch (Exception e){
            log.error("读取配置文件有误："+e);
        }
    }

}
