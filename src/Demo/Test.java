package Demo;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.util.*;

/**
 * 取出Oracle/Hive SQL 中涉及到的所有源表字段
 */
public class Test {


    public static void main(String[] args) throws Exception {

        /**
         * sql文件路径，请自己填写
         * 目前已知BUG：'UNION ALL' 的前后最好补上空格，否则解析时容易报错
         *
         *
         */
        File file = new File("input\\");

        // map的key为表名，value为该表使用到的所有列名
        Map<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
        // 数据库类型 (Oracle)
        String dbType = JdbcConstants.ORACLE;

        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            // 读取sql文件中的sql
            StringBuilder sql = new StringBuilder("");
            BufferedReader reader = null;
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(files[i]), "GB2312"));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sql.append(tempStr + "\n");
            }
            reader.close();
            System.out.println(files[i].getName());
            // 获取sql语句中所有表和字段
            getTableAndColumnBySql(sql.toString(), dbType, map);
        }


        // 输出map所有键值对 格式根据需要自己调整
        Set<String> set = map.keySet();
        File dir = new File("output");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File f = new File("output\\模型.txt");
        if (f.exists()) {
            f.delete();
        }
        FileOutputStream fos1 = new FileOutputStream(f);
        OutputStreamWriter dos1 = new OutputStreamWriter(fos1);
        for (String o : set) {
            for (String s : map.get(o)) {
                if (s.equals("*")) {
                    throw new Exception("***");
                }
                String oo = o;
//                oo = o.replaceAll("SOOCHOW_DATA\\.ODS_", "");
//                oo = oo.replaceFirst("_", "\\.");
                dos1.write(oo + "." + s + "\n");
            }
        }
        dos1.close();
        fos1.close();
        System.out.println();

        return;
    }

    /**
     * 获取sql语句中所有表和字段
     *
     * @param sql
     * @param dbType
     * @param map
     * @return
     */
    private static void getTableAndColumnBySql(String sql, String dbType, Map<String, ArrayList<String>> map) {
        // 格式化sql
//        sql = SQLUtils.format(sql, dbType);
        // 解析sql，生成 AST(抽象语法树)
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        if (CollectionUtils.isEmpty(stmtList)) {
            System.out.printf("stmtList为空");
        }
        for (SQLStatement sqlStatement : stmtList) {
            // 使用 visitor 访问 AST(抽象语法树)
            OracleSchemaStatVisitor visitor = new OracleSchemaStatVisitor();
            sqlStatement.accept(visitor);

            // visitor.getColumns() 即是所有用到的表和字段
            for (TableStat.Column c : visitor.getColumns()) {
                if (c.getTable().toUpperCase().equals("UNKNOWN")) {
                    System.out.println("aaa");
                }
                if (c.getName().toUpperCase().equals("产品代码")) {
                    System.out.println("BBB");
                }
                // 没有添加的表，新建list
                if (!map.containsKey(c.getTable().toUpperCase())) {
                    ArrayList<String> colList1 = new ArrayList<String>();
                    colList1.add(c.getName().toUpperCase());
                    map.put(c.getTable().toUpperCase(), colList1);
                } else {
                    // 添加过的表，直接add
                    ArrayList<String> colList2 = map.get(c.getTable().toUpperCase());
                    if (colList2.contains((c.getName().toUpperCase()))) {
                        continue;
                    }
                    colList2.add(c.getName().toUpperCase());
                }
//                System.out.println(c);
            }
        }
    }
}
