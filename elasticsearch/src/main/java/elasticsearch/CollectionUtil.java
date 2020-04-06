package elasticsearch;

import java.util.Collection;
import java.util.List;
import java.util.Map;
/**
* CollectionUtil
*/
public class CollectionUtil {
	
	/** 
     * 定义分割常量 （#在集合中的含义是每个元素的分割，|主要用于map类型的集合用于key与value中的分割） 
     */  
    private static final String SEP1 = "#";  
    private static final String SEP2 = "|"; 
    private static final String SEP3 = ","; 
	
	/**
	 * Return {@code true} if the supplied Collection is {@code null} or empty.
	 * Otherwise, return {@code false}.
	 * 
	 * @param collection
	 *            the Collection to check
	 * @return whether the given Collection is empty
	 */
	@SuppressWarnings("all")
	public static boolean isEmpty(Collection collection) {
		return (collection == null || collection.isEmpty());
	}

	/**
	 * Return {@code true} if the supplied Map is {@code null} or empty.
	 * Otherwise, return {@code false}.
	 * 
	 * @param map
	 *            the Map to check
	 * @return whether the given Map is empty
	 */
	@SuppressWarnings("all")
	public static boolean isEmpty(Map map) {
		return (map == null || map.isEmpty());
	}

	public static String changeListToSql(List<String> list) {
		StringBuffer str = new StringBuffer();
		int listSize = list.size();
		for (int i = 0; i < listSize; i++) {
			str.append("'" + list.get(i) + "'");
			if ((i + 1) != listSize) {
				str.append(",");
			}
		}
		return str.toString();
	}
	
	/** 
     * List转换String 
     *  
     * @param list 
     *            :需要转换的List 
     * @return String转换后的字符串 
     */  
    public static String List2String(List<?> list) {  
        StringBuffer sb = new StringBuffer();  
        if (list != null && list.size() > 0) {  
            for (int i = 0; i < list.size(); i++) {  
                if (list.get(i) == null || list.get(i).equals("")) {  
                    continue;  
                }  
                // 如果值是list类型则调用自己  
                if (list.get(i) instanceof List) {  
                    sb.append(List2String((List<?>) list.get(i)));  
                    sb.append(SEP1);  
                } else if (list.get(i) instanceof Map) {  
                    sb.append(Map2String((Map<?, ?>) list.get(i)));  
                    sb.append(SEP1);  
                } else {  
                    sb.append(list.get(i));  
                    sb.append(SEP1);  
                }  
            }  
        }  
        return sb.toString();  
    }  
	
	/** 
     * Map转换String 
     *  
     * @param map 
     *            :需要转换的Map 
     * @return String转换后的字符串 
     */  
    public static String Map2String(Map<?, ?> map) {  
        StringBuffer sb = new StringBuffer();  
        // 遍历map  
        for (Object obj : map.keySet()) {  
            if (obj == null) {  
                continue;  
            }  
            Object key = obj;  
            Object value = map.get(key);  
            if (value instanceof List<?>) {  
                sb.append(key.toString() + SEP1 + List2String((List<?>) value));  
                sb.append(SEP2);  
            } else if (value instanceof Map<?, ?>) {  
                sb.append(key.toString() + SEP1  
                        + Map2String((Map<?, ?>) value));  
                sb.append(SEP2);  
            } else {  
                sb.append(key.toString() + SEP1 + value.toString());  
                sb.append(SEP2);  
            }  
        }  
        return sb.toString();  
    } 
    

	/** 
     * Object数组转换String 
     *  
     * @param objs 
     *            :需要转换的Object数组
     * @return String转换后的字符串 
     */  
    public static String ObjectArray2String(Object[] objs) {
        StringBuffer sb = new StringBuffer();
        if (objs != null && objs.length > 0) {
            for (int i = 0; i < objs.length; i++) {
            	Object obj = objs[i];
                if (obj == null) {
                    continue;
                }
                sb.append(obj.toString());
                sb.append(SEP3);
            }
        }
        return sb.toString();
    }
	/** 
     * Class数组转换String 
     *  
     * @param clas
     *            :需要转换的Class数组
     * @return String转换后的字符串 
     */  
    @SuppressWarnings("rawtypes")
	public static String ClassArray2String(Class[] clas) {
        StringBuffer sb = new StringBuffer();
        if (clas != null && clas.length > 0) {
            for (int i = 0; i < clas.length; i++) {
            	Class cla = clas[i];
                if (cla == null) {
                    continue;
                }
                sb.append(cla.toString());
                sb.append(SEP3);
            }
        }
        return sb.toString();
    }
}
