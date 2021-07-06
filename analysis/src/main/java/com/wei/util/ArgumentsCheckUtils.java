package com.wei.util;

import com.wei.Exception.CommonException;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 参数校验utils类
 */
public class ArgumentsCheckUtils {

    /**
     * 检查对象是否非空
     *
     * @param t
     * @param <T>
     */
    public static <T> void checkNotNull(T t) throws CommonException {
        if (t == null) {
            throw new CommonException(1001,"对象为空");
        }
    }

    /**
     * list是否为空
     * @param list
     * @param <T>
     * @throws CommonException
     */
    public static <T> void checkListNotNull(List<T> list) throws CommonException {
        if (CollectionUtils.isEmpty(list)) {
            throw new CommonException(1002,"列表为空");
        }
    }

    /**
     * 检查string是否是空字符串
     *
     * @param s
     * @throws CommonException
     */
    public static void checkStrNotEmpty(String s) throws CommonException {
        if (StringUtils.isEmpty(s)) {
            throw new CommonException(1003,"字符串为空");
        }
    }

    /**
     * 检测数值是否是正数-Long
     *
     * @param value
     * @throws CommonException
     */
    public static void checkPositiveNumber(Long value) throws CommonException {
        checkNotNull(value);
        if (value <= 0) {
            throw new CommonException(1004,"数值为负数");
        }
    }


}
