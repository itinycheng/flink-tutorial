flink-learn
================
在Flink学习过程中落地的一些代码，主要是Flink官方的Demo和扩展思考；

### 构建环境

- `Oracle JDK-1.8.0_144`
- `Scala-2.11.8`
- `apache-maven-3.5.0`
- `Flink-1.6.0`
- `IntelliJ IDEA-2018.1`

### 模块说明

- flink-learn
    - api：flink api的使用，包含 `operator`, `window`, `state`,`sql`, etc.

### 编译说明：

api：

- 打jar包上次至远程集群运行  
首先，确保`pom.xml`中的变量`flink.scope`配置为`provided`，其目的在于在打包过程中排除flink相关依赖（这些依赖已经在flink集群中存在）；  
其次，跳转到项目跟目录中执行`mvn clean package -DskipTests`，在`api/target`下就可找到jar包。
最后，将jar上传到远程集群并运行；

- 本地调试
首先，将`pom.xml`中的变量`flink.scope`配置为`compile`；
其次，跳转到项目跟目录中执行`mvn clean compile -DskipTests`；
最后，找到要调试类的`main`函数进行debug；

- 注 ：当前代码用`socket`作为`flink job`的`source`，为方便调试可先安装`netcat`工具并用命令`nc -l -p 12345`来开启`socket`监听端口。
