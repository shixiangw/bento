package iotdb

import (
	"context"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/warpstreamlabs/bento/public/service"
)

// 基于 IoTDB 的特性，我们首先定义配置结构
func iotdbOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("将数据写入 IoTDB 数据库").
		Description("将消息写入 IoTDB 时间序列数据库").
		Categories("Database").
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField("batching"),
			service.NewStringField("address").
				Description("IoTDB 服务器地址").
				Example("localhost:6667").
				Default("localhost:6667"),
			service.NewStringField("username").
				Description("用户名").
				Default("root"),
			service.NewStringField("password").
				Description("密码").
				Default("root").
				Secret(),
			service.NewStringField("database").
				Description("数据库名称").
				Example("root.demo").
				Optional(),
			service.NewDurationField("timeout").
				Description("连接超时时间").
				Default("10s").
				Advanced(),
		)
}

type iotdbWriter struct {
	conf     *service.ParsedConfig
	mgr      *service.Resources
	log      *service.Logger
	address  string
	username string
	password string
	database string
	timeout  time.Duration
	client   client.Session
}

func init() {
	if err := service.RegisterBatchOutput(
		"iotdb",
		iotdbOutputConfig(),
		fromConf,
	); err != nil {
		panic(err)
	}
}

func fromConf(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
	var address, username, password, database string
	var timeout time.Duration

	if address, err = conf.FieldString("address"); err != nil {
		return
	}
	if username, err = conf.FieldString("username"); err != nil {
		return
	}
	if password, err = conf.FieldString("password"); err != nil {
		return
	}
	if conf.Contains("database") {
		if database, err = conf.FieldString("database"); err != nil {
			return
		}
	}
	if timeout, err = conf.FieldDuration("timeout"); err != nil {
		return
	}
	if batchPol, err = conf.FieldBatchPolicy("batching"); err != nil {
		return
	}
	if mif, err = conf.FieldMaxInFlight(); err != nil {
		return
	}

	w := &iotdbWriter{
		conf:     conf,
		mgr:      mgr,
		log:      mgr.Logger(),
		address:  address,
		username: username,
		password: password,
		database: database,
		timeout:  timeout,
	}

	out = w
	return
}

func (i *iotdbWriter) Connect(ctx context.Context) error {
	// 解析地址获取主机和端口
	host, port := "localhost", "6667"
	if i.address != "" {
		// 简单解析地址格式 host:port
		addrParts := strings.Split(i.address, ":")
		if len(addrParts) == 2 {
			host = addrParts[0]
			port = addrParts[1]
		}
	}

	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: i.username,
		Password: i.password,
	}

	i.client = client.NewSession(config)
	if err := i.client.Open(false, 0); err != nil {
		i.log.Errorf("无法连接到 IoTDB: %v", err)
		return err
	}

	i.log.Info("IoTDB 连接成功")
	return nil
}

func (i *iotdbWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	// 批量写入 IoTDB
	// 遍历消息批次，将每条消息写入 IoTDB
	return batch.WalkWithBatchedErrors(func(index int, msg *service.Message) error {
		// 获取消息内容
		content, err := msg.AsBytes()
		if err != nil {
			i.log.Errorf("无法获取消息内容: %v", err)
			return err
		}

		// 将消息内容作为字符串插入到 IoTDB
		// 这里我们使用一个简单的示例，实际应用中可能需要根据消息格式进行解析
		deviceId := "root.bento.messages"
		measurements := []string{"content"}
		values := []interface{}{string(content)}
		dataTypes := []client.TSDataType{client.TEXT}
		timestamp := time.Now().UnixNano() / 1000000 // 毫秒时间戳

		_, err = i.client.InsertRecord(deviceId, measurements, dataTypes, values, timestamp)
		if err != nil {
			i.log.Errorf("写入 IoTDB 失败: %v", err)
			return err
		}

		i.log.Debugf("写入 IoTDB 的消息 %d: %s", index, string(content))
		return nil
	})
}

func (i *iotdbWriter) Close(ctx context.Context) error {
	// 关闭 IoTDB 连接
	i.client.Close()

	i.log.Info("IoTDB 连接已关闭")
	return nil
}
