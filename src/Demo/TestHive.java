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
 * Created by Lululululu on 2021-05-10.
 */
public class TestHive {


    public static void main(String[] args) throws Exception {

        /**
         * sql文件路径，请自己填写
         * 目前已知BUG：'UNION ALL' 的前后最好补上空格，否则解析时容易报错
         *
         *
         */
        File file = new File("C:\\Users\\NJY_SHIP\\Downloads\\input\\");

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
        Set set = map.keySet();
        File f = new File("C:\\Users\\NJY_SHIP\\Downloads\\output\\模型.txt");
        if (f.exists()) {
            f.delete();
        }
        FileOutputStream fos1 = new FileOutputStream(f);
        OutputStreamWriter dos1 = new OutputStreamWriter(fos1);
        for (Object o : set) {
            dos1.write(o + ":" + map.get(o) + "\n");
        }
        dos1.close();
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
            for (SQLSelectItem sqlSelectItem : sqlSelectItems) {
                SQLExpr sqlExpr = sqlSelectItem.getExpr();
                System.out.println(sqlSelectItem.getAlias());


                List<String> usedCol = new ArrayList<>(getColInfo(sqlExpr));


                Set<String> usedColSet = new HashSet<String>(usedCol);
//                for (String usedCol1 : usedColSet) {
//                    System.out.print(usedCol1);
//                }
                System.out.println(usedColSet);
                System.out.println();

            }

        }
    }

    private static List<String> getColInfo(SQLExpr sqlExpr) throws Exception {
        List<String> subUsedCol = new ArrayList<>();
        if (sqlExpr instanceof SQLCharExpr) {
            /* 字符串 */

        } else if (sqlExpr instanceof SQLIntegerExpr) {
            /* 数字 */

        } else if (sqlExpr instanceof SQLBinaryExpr) {
            /* 真假 */

        } else if (sqlExpr instanceof SQLCaseExpr) {
            /* 控制流 CASE */
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
        } else if (sqlExpr instanceof SQLBinaryOpExpr) {
            /* 真假判断 */
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
            // 左边
            subUsedCol.addAll(getColInfo(sqlBinaryOpExpr.getLeft()));
            // 右边
            subUsedCol.addAll(getColInfo(sqlBinaryOpExpr.getRight()));
        } else if (sqlExpr instanceof SQLMethodInvokeExpr) {
            /* 方法 function */
            SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) sqlExpr;
            String functionName = sqlMethodInvokeExpr.getMethodName().toLowerCase();
            List<String> functionList = Arrays.asList("if", "substr", "from_unixtime", "unix_timestamp", "");
            if (!functionList.contains(functionName)) {
                throw new Exception("有未适配的表达式出现了！！！！！！");
            }
            for (SQLExpr sqlExpr1 : sqlMethodInvokeExpr.getParameters()) {
                subUsedCol.addAll(getColInfo(sqlExpr1));
            }
        } else if (sqlExpr instanceof SQLIdentifierExpr) {
            /* 标识符 */
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
            if (sqlIdentifierExpr.getResolvedOwnerObject() != null) {
                SQLObject resolvedOwnerObject = sqlIdentifierExpr.getResolvedOwnerObject();
                subUsedCol.addAll(getResolvedOwnerObject(resolvedOwnerObject, sqlIdentifierExpr.getName()));
            } else {
                throw new Exception("有未适配的表达式出现了！！！！！！");
            }
        } else if (sqlExpr instanceof SQLPropertyExpr) {
            /* 属性 */
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
