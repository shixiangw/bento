# Bento 启动 IoTDB Sink 的业务流程

## 启动流程图

```mermaid
graph LR
    A[Bento启动] --> B[解析配置文件]
    B --> C{包含IoTDB配置?}
    C -->|是| D[创建IoTDB Writer]
    C -->|否| E[忽略IoTDB组件]
    D --> F[注册输出组件]
    F --> G[Bento启动完成]
    
    subgraph 数据处理阶段
        G --> H[接收数据消息]
        H --> I[调用Connect方法]
        I --> J[建立IoTDB连接]
        J --> K[批量处理消息]
        K --> L[写入IoTDB数据库]
        L --> M{写入成功?}
        M -->|是| N[确认处理]
        M -->|否| O[记录错误]
    end
```

## 组件初始化流程

```mermaid
sequenceDiagram
    participant B as Bento主程序
    participant W as IoTDB Writer
    participant I as IoTDB服务器
    
    B->>W: 初始化配置
    W->>W: 解析配置参数
    W->>W: 创建Session对象
    B->>W: 注册为输出组件
    B->>B: 启动完成
    
    Note over B,I: 数据处理阶段开始
    
    B->>W: 发送数据批次
    W->>W: 检查连接状态
    W->>I: 建立连接(首次)
    I-->>W: 连接成功
    W->>W: 准备数据记录
    W->>I: 批量插入数据
    I-->>W: 插入结果
    W->>B: 返回处理状态
```

## 关键步骤说明

### 1. 配置解析阶段
- Bento 启动时读取 YAML 配置文件
- 检测是否存在 `iotdb` 输出配置块
- 解析配置参数：地址、用户名、密码、数据库名、超时等

### 2. 组件初始化阶段
- 创建 `iotdbWriter` 结构体实例
- 初始化 IoTDB 客户端 Session
- 注册为 Bento 输出组件

### 3. 连接建立阶段
- 首次接收到数据时调用 `Connect` 方法
- 使用配置信息建立与 IoTDB 的连接
- 保持长连接以提高性能

### 4. 数据处理阶段
- 接收来自上游的数据消息
- 根据批处理配置积累消息
- 将消息转换为 IoTDB 记录格式
- 调用 IoTDB 客户端 API 插入数据
- 返回处理结果给 Bento 核心引擎

这个流程确保了 Bento 能够稳定、高效地将数据写入 IoTDB 时间序列数据库。