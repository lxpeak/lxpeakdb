# MYDB

MYDB 是一个 Java 实现的简单的数据库。目前有以下功能：

- 日志记录和数据恢复
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简单的 SQL 解析
- 基于 socket 的 server 和 client

## 运行方式

注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先执行以下命令编译源码：

```shell
mvn compile
```

接着执行以下命令以 D://mydb/ 作为路径创建数据库：

```shell
mvn exec:java -Dexec.mainClass="com.lxpeak.mydb.backend.Launcher" -Dexec.args="-create D://mydb/db1"
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -Dexec.mainClass="com.lxpeak.mydb.backend.Launcher" -Dexec.args="-open D://mydb/db1"
```

这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -Dexec.mainClass="com.lxpeak.mydb.client.Launcher"
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

一个执行示例：

```shell
# 建学生表，索引为id和name
create table students id int32, name string, age int32 (index id name)
    
# 插入数据
insert into students values 1 "ZhangSan" 18

# 查询数据
select * from students where id = 1
```

目前只支持基于索引查找数据，不支持全表扫描。

