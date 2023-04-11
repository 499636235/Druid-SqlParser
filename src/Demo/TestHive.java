package Demo;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
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
 * 取出HiveSQL中每个字段的源表字段
 */
public class TestHive {


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
        String dbType = JdbcConstants.HIVE;

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
//        Set set = map.keySet();
//        File f = new File("C:\\Users\\NJY_SHIP\\Downloads\\output\\模型.txt");
//        if (f.exists()) {
//            f.delete();
//        }
//        FileOutputStream fos1 = new FileOutputStream(f);
//        OutputStreamWriter dos1 = new OutputStreamWriter(fos1);
//        for (Object o : set) {
//            dos1.write(o + ":" + map.get(o) + "\n");
//        }
//        dos1.close();
//        System.out.println();

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
    private static void getTableAndColumnBySql(String sql, String dbType, Map<String, ArrayList<String>> map) throws Exception {
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

            SQLSelect select = (SQLSelect) sqlStatement.getChildren().get(0);
            SQLWithSubqueryClause sqlWithSubqueryClause = select.getWithSubQuery();
            SQLSelectQueryBlock queryBlock = select.getQueryBlock();
            List<SQLSelectItem> sqlSelectItems = queryBlock.getSelectList();


            File dir = new File("output");
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File f = new File("output\\1.txt");
            if (f.exists()) {
                f.delete();
            }
            FileOutputStream fos1 = new FileOutputStream(f);
            OutputStreamWriter dos1 = new OutputStreamWriter(fos1);

            StringBuilder sb = new StringBuilder();

            for (SQLSelectItem sqlSelectItem : sqlSelectItems) {
                SQLExpr sqlExpr = sqlSelectItem.getExpr();


                if (sqlSelectItem.getAlias() != null) {
//                    System.out.print(sqlSelectItem.getAlias());
                    sb.append(sqlSelectItem.getAlias());
                } else {
                    SQLExpr expr = sqlSelectItem.getExpr();
                    if (expr instanceof SQLPropertyExpr) {
//                        System.out.print(((SQLPropertyExpr) expr).getName());
                        sb.append(((SQLPropertyExpr) expr).getName());
                    } else if (expr instanceof SQLIdentifierExpr) {
//                        System.out.print(((SQLIdentifierExpr) expr).getName());
                        sb.append(((SQLIdentifierExpr) expr).getName());

                    }
                }


                List<String> usedCol = new ArrayList<>(getColInfo(sqlExpr));


                Set<String> usedColSet = new HashSet<String>(usedCol);

                if (usedColSet.size() == 1) {
                    for (String usedCol2 : usedColSet) {

                        usedCol2 = usedCol2.replaceAll("soochow_data\\.ods_", "");
                        usedCol2 = usedCol2.replaceFirst("_", "\\.");

//                        System.out.print("\t\t\t\t\t\t" + usedCol2);
                        sb.append("\t\t\t\t\t\t" + usedCol2);
                    }
                }

//                System.out.println(usedColSet);

//                System.out.println();
                sb.append("\n");

            }

            dos1.write(String.valueOf(sb));
            dos1.close();
            fos1.close();

        }
    }

    /**
     * SQLIdentifierExpr：用于表示 SQL 中的标识符，例如表名、列名等。
     * SQLPropertyExpr：用于表示 SQL 中的属性访问，例如表达式中的表别名和列名的结合。
     * SQLAggregateExpr：用于表示 SQL 中的聚合函数，例如 SUM、AVG、MAX、MIN 等。
     * SQLLiteralExpr：用于表示 SQL 中的字面量，例如字符串、数字、日期等。
     * SQLVariantRefExpr：用于表示 SQL 中的占位符，例如 JDBC 中的参数占位符。
     * SQLBinaryOpExpr：用于表示 SQL 中的二元操作符，例如加法、减法、乘法、除法等。
     * SQLUnaryExpr：用于表示 SQL 中的一元操作符，例如取负数、逻辑非等。
     * SQLCaseExpr：用于表示 SQL 中的 CASE 表达式。
     * SQLInListExpr：用于表示 SQL 中的 IN 子句。
     * SQLExistsExpr：用于表示 SQL 中的 EXISTS 子查询。
     *
     * @param sqlExpr
     * @return
     */
    private static List<String> getColInfo(SQLExpr sqlExpr) throws Exception {
        List<String> subUsedCol = new ArrayList<>();
        if (sqlExpr instanceof SQLCharExpr) {
            /* 字符串 */

        } else if (sqlExpr instanceof SQLIntegerExpr) {
            /* 数字 */

        } else if (sqlExpr instanceof SQLBinaryExpr) {
            /* 真假 */

        } else if (sqlExpr instanceof SQLLiteralExpr) {
            /* 字面量，例如字符串、数字、日期等 */

        } else if (sqlExpr instanceof SQLInListExpr) {
            /* in ('') */
            SQLInListExpr sqlInListExpr = (SQLInListExpr) sqlExpr;
            subUsedCol.addAll(getColInfo(sqlInListExpr.getExpr()));
            for (SQLExpr sqlExpr2 : sqlInListExpr.getTargetList()) {
                subUsedCol.addAll(getColInfo(sqlExpr2));
            }
        } else if (sqlExpr instanceof SQLBinaryOpExpr) {
            /* 二元操作符，例如加法、减法、乘法、除法等 */
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
            // 左边
            subUsedCol.addAll(getColInfo(sqlBinaryOpExpr.getLeft()));
            // 右边
            subUsedCol.addAll(getColInfo(sqlBinaryOpExpr.getRight()));
        } else if (sqlExpr instanceof SQLCaseExpr) {
            /* 控制流 CASE 表达式 */
            SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) sqlExpr;
            if (sqlCaseExpr.getValueExpr() != null) {
                subUsedCol.addAll(getColInfo(sqlCaseExpr.getValueExpr()));
            }
            if (sqlCaseExpr.getElseExpr() != null) {
                subUsedCol.addAll(getColInfo(sqlCaseExpr.getElseExpr()));
            }
            List<SQLCaseExpr.Item> items = sqlCaseExpr.getItems();
            if (items != null) {
                for (SQLCaseExpr.Item item : items) {
                    subUsedCol.addAll(getColInfo(item.getConditionExpr()));
                    subUsedCol.addAll(getColInfo(item.getValueExpr()));
                }
            }
        } else if (sqlExpr instanceof SQLMethodInvokeExpr) {
            /* 方法 function */
            SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) sqlExpr;
            String functionName = sqlMethodInvokeExpr.getMethodName().toLowerCase();
            List<String> functionList = Arrays.asList("if", "substr", "from_unixtime", "unix_timestamp", "nvl", "");
            if (!functionList.contains(functionName)) {
                throw new Exception("有未适配的表达式出现了！！！！！！");
            }
            for (SQLExpr sqlExpr1 : sqlMethodInvokeExpr.getParameters()) {
                subUsedCol.addAll(getColInfo(sqlExpr1));
            }
        } else if (sqlExpr instanceof SQLIdentifierExpr) {
            /* 标识符，例如表名、列名等 */
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
            if (sqlIdentifierExpr.getResolvedOwnerObject() != null) {
                SQLObject resolvedOwnerObject = sqlIdentifierExpr.getResolvedOwnerObject();
                subUsedCol.addAll(getResolvedOwnerObject(resolvedOwnerObject, sqlIdentifierExpr.getName()));
            } else {
                throw new Exception("有未适配的表达式出现了！！！！！！");
            }
        } else if (sqlExpr instanceof SQLPropertyExpr) {
            /* 属性访问，例如表达式中的表别名和列名的结合 */
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExpr;

            if (sqlPropertyExpr.getResolvedOwnerObject() != null) {
                SQLObject resolvedOwnerObject = sqlPropertyExpr.getResolvedOwnerObject();
                subUsedCol.addAll(getResolvedOwnerObject(resolvedOwnerObject, sqlPropertyExpr.getName()));
            } else {
                if (sqlPropertyExpr.getOwner() != null) {
                    subUsedCol.add(sqlPropertyExpr.getOwner() + "." + sqlPropertyExpr.getName());
                } else {
                    throw new Exception("有未适配的表达式出现了！！！！！！");
                }
            }
        } else if (sqlExpr instanceof SQLQueryExpr) {
            /* 子查询 */

        } else {
            throw new Exception("有未适配的表达式出现了！！！！！！");
        }
        return subUsedCol;
    }

    public static List<String> getResolvedOwnerObject(SQLObject resolvedOwnerObject, String colname) throws Exception {
        List<String> subUsedCol = new ArrayList<>();
        if (resolvedOwnerObject instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) resolvedOwnerObject;
            SQLExpr expr = sqlExprTableSource.getExpr();

            if (expr instanceof SQLPropertyExpr) {
                SQLPropertyExpr sqlPropertyExpr1 = (SQLPropertyExpr) expr;
                subUsedCol.add(sqlPropertyExpr1.getOwner() + "." + sqlPropertyExpr1.getName() + "." + colname);
            } else if (expr instanceof SQLIdentifierExpr) {
                subUsedCol.addAll(getColInfo(expr));
            } else {
                throw new Exception("有未适配的表达式出现了！！！！！！");
            }
        } else if (resolvedOwnerObject instanceof SQLWithSubqueryClause.Entry) {
            /* With子查询 */
//            SQLWithSubqueryClause.Entry sqlWithSubqueryClause = (SQLWithSubqueryClause.Entry) resolvedOwnerObject;
//            SQLExprTableSource from = (SQLExprTableSource) sqlWithSubqueryClause.getSubQuery().getQueryBlock().getFrom();
//            subUsedCol.addAll(getColInfo(from.getExpr()));
            subUsedCol.add("with子查询暂时不支持");
        } else if (resolvedOwnerObject instanceof SQLSubqueryTableSource) {
            /* 子查询 */

            subUsedCol.add("子查询暂时不支持");
        } else {
            throw new Exception("有未适配的表达式出现了！！！！！！");
        }
        return subUsedCol;
    }
}
