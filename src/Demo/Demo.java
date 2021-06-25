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
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;


import java.io.*;
import java.util.*;


public class Demo {


    public static void main(String[] args) throws Exception {

        /**
         * sql文件路径，请自己填写
         * 目前已知BUG：'UNION ALL' 的前后最好补上空格，否则解析时容易报错
         *
         *
         */
        File file = new File("/Users/njy/Documents/zkr/新数据中台/快速报表sql/个险营销员销售团险业务清单.sql");


        // 读取sql文件中的sql
        StringBuilder sql = new StringBuilder("");
        BufferedReader reader = null;
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GB2312"));
        String tempStr;
        while ((tempStr = reader.readLine()) != null) {
            sql.append(tempStr);
        }
        reader.close();


        // 数据库类型 (Oracle)
        String dbType = JdbcConstants.ORACLE;

        // 获取sql语句中所有表和字段
        getTableAndColumnBySql(sql.toString(), dbType);


//        String sql = "SELECT (SELECT LP.NAME\n" +
//                "                     FROM LIS.LABRANCHGROUP LP\n" +
//                "                    WHERE LP.AGENTGROUP = P.UPBRANCH) as njy\n" +
//                "             FROM LIS.LACOMMISIONDETAIL D, LIS.LAAGENT L, LIS.LABRANCHGROUP P\n" +
//                "            WHERE D.AGENTCODE = L.AGENTCODE\n" +
//                "              AND D.GRPCONTNO = A.GRPCONTNO\n" +
//                "              AND L.BRANCHCODE = P.AGENTGROUP\n" +
//                "              AND L.BRANCHTYPE IN ('1', '4')\n" +
//                "              AND ROWNUM = 1";
//        List<String> strings = getSelectColumns(sql, dbType);
//        System.out.printf(strings.toString());

        return;
    }

    /**
     * 获取sql语句中所有表和字段
     *
     * @param sql
     * @param dbType
     * @return
     */
    private static void getTableAndColumnBySql(String sql, String dbType) {
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
            // map的key为表名，value为该表使用到的所有列名
            Map<String, ArrayList<String>> map = new HashMap<>();
            // visitor.getColumns() 即是所有用到的表和字段
            for (TableStat.Column c : visitor.getColumns()) {
                // 没有添加的表，新建list
                if (!map.containsKey(c.getTable())) {
                    ArrayList<String> colList1 = new ArrayList<>();
                    colList1.add(c.getName());
                    map.put(c.getTable(), colList1);
                } else {
                    // 添加过的表，直接add
                    ArrayList<String> colList2 = map.get(c.getTable());
                    colList2.add(c.getName());
                }
//                System.out.println(c);
            }
            // 输出map所有键值对 格式根据需要自己调整
            Set set = map.keySet();
            System.out.println();
            for (Object o : set) {

                /**
                 * 格式：LIS.LDCODE	CODENAME	CODE	CODETYPE
                 */
                System.out.print(o);
                for (String co : map.get(o)) {
                    System.out.print("\t" + co);
                }
                System.out.println();


                /**
                 * 格式：LIS.LDCODE:[CODENAME, CODE, CODETYPE]
                 */
//                System.out.println(o + ":" + map.get(o));

            }
            System.out.println();
        }
    }


    private static List<String> getTableNameBySql(String sql, String dbType0) {
        String dbType = dbType0;
        try {
            List<String> tableNameList = new ArrayList<>();
            //格式化输出
            String sqlResult = SQLUtils.format(sql, dbType);
//            System.out.printf("格式化后的sql:[{}]",sqlResult);

            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            if (CollectionUtils.isEmpty(stmtList)) {
                System.out.printf("stmtList为空无需获取");
                return Collections.emptyList();
            }
            for (SQLStatement sqlStatement : stmtList) {
                OracleSchemaStatVisitor visitor = new OracleSchemaStatVisitor();
                sqlStatement.accept(visitor);

//                System.out.println(visitor.getColumns());
                for (TableStat.Column c : visitor.getColumns()) {
                    System.out.println(c.toString());
                }
//                System.out.println(visitor.getOrderByColumns());

                Map<TableStat.Name, TableStat> tables = visitor.getTables();
//                System.out.printf("druid解析sql的结果集:[{}]",tables);
                Set<TableStat.Name> tableNameSet = tables.keySet();
                for (TableStat.Name name : tableNameSet) {
                    String tableName = name.getName();
                    if (StringUtils.isNotBlank(tableName)) {
                        tableNameList.add(tableName);
                    }
                }
            }
//            System.out.printf("解析sql后的表名:[{}]",tableNameList);
            return tableNameList;
        } catch (Exception e) {
            System.out.printf("**************异常SQL:[{}]*****************\n", sql);
            System.out.printf(e.getMessage(), e);
        }
        return Collections.emptyList();
    }


    /**
     * 获取sql语句中查询字段
     *
     * @param sql
     * @param jdbcType
     * @return
     */
    public static List<String> getSelectColumns(String sql, String jdbcType) { //类型转换
        List<String> columns = new ArrayList();
        //格式化sql语句
//        String sql = SQLUtils.format(sqlOld, jdbcType);
        if (sql.contains("*")) {
            throw new RuntimeException("不支持语句中带 '*' ，必须明确指定查询的列");
        }
        // parser得到AST
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(
                sql, jdbcType);
        //只接受select 语句
        if (!Token.SELECT.equals(parser.getExprParser().getLexer().token())) {
            throw new RuntimeException("不支持 " + parser.getExprParser().getLexer().token() + " 语法,仅支持 SELECT 语法");
        }
        List<SQLStatement> stmtList = parser.parseStatementList();
        if (stmtList.size() > 1) {
            throw new RuntimeException("不支持多条SQL语句,当前是" + stmtList.size() + "条语句");
        }


        //接收查询字段
        List<SQLSelectItem> items = null;
        for (SQLStatement stmt : stmtList) {
            if (stmt instanceof SQLSelectStatement) {
                SQLSelectStatement sstmt = (SQLSelectStatement) stmt;
                SQLSelect sqlselect = sstmt.getSelect();
                SQLSelectQueryBlock query = (SQLSelectQueryBlock) sqlselect.getQuery();
                items = query.getSelectList();
                SQLTableSource tableSource = query.getFrom();
            }
        }
        for (SQLSelectItem s : items) {
            String column = s.getAlias();
//            String column = StringUtils.isEmpty(s.getAlias()) ? expr.toString() : s.getAlias();
            //防止字段重复
            if (!columns.contains(column)) {
                columns.add(column);
            }
        }
        return columns;
    }

}
