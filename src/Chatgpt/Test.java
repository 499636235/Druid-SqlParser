import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.List;

public class Test {
    public static void main(String[] args) {
        String sql = "SELECT name, position FROM employees";
        try {
            // 使用 MySQL 语法解析器解析 SELECT 语句
            MySqlStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatement();
            // 获取 SELECT 语句中的列名
            List<SQLSelectItem> columns = stmt.getSelect().getQueryBlock().getSelectList();
            for (SQLSelectItem column : columns) {
                String columnStr = SQLUtils.toMySqlString(column.getExpr());
                System.out.println("Column: " + columnStr);
            }
            // 获取 SELECT 语句中的表名
            SQLTableSource from = stmt.getSelect().getQueryBlock().getFrom();
            StringBuilder sb = new StringBuilder();
            MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb);
            from.accept(visitor);
            String tableName = sb.toString();
            System.out.println("Table Name: " + tableName);
        } catch (Exception e) {
            e.printStackTrace();
            // 处理异常
        }
    }
}