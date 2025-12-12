package iotdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestIoTDBConfigParsing(t *testing.T) {
	configSpec := iotdbOutputConfig()
	conf := `
address: "localhost:6667"
username: "root"
password: "root"
database: "root.demo"
timeout: "15s"
`
	parsed, err := configSpec.ParseYAML(conf, nil)
	require.NoError(t, err)

	out, _, _, err := fromConf(parsed, service.MockResources())
	require.NoError(t, err)

	i, ok := out.(*iotdbWriter)
	require.True(t, ok)

	require.Equal(t, "localhost:6667", i.address)
	require.Equal(t, "root", i.username)
	require.Equal(t, "root", i.password)
	require.Equal(t, "root.demo", i.database)
	require.Equal(t, 15*time.Second, i.timeout)

	// 测试默认值
	conf2 := `
address: "127.0.0.1:7777"
username: "admin"
password: "admin"
`
	parsed2, err := configSpec.ParseYAML(conf2, nil)
	require.NoError(t, err)

	out2, _, _, err := fromConf(parsed2, service.MockResources())
	require.NoError(t, err)

	i2, ok := out2.(*iotdbWriter)
	require.True(t, ok)

	require.Equal(t, "127.0.0.1:7777", i2.address)
	require.Equal(t, "admin", i2.username)
	require.Equal(t, "admin", i2.password)
	// 数据库是可选的，不应该有默认值
	require.Equal(t, "", i2.database)
}

// TestIoTDBWrite 测试 IoTDB 写入功能的基本结构
func TestIoTDBWrite(t *testing.T) {
	// 创建一个 IoTDB writer
	configSpec := iotdbOutputConfig()
	conf := `
address: "localhost:6667"
username: "root"
password: "root"
`
	parsed, err := configSpec.ParseYAML(conf, nil)
	require.NoError(t, err)

	out, _, _, err := fromConf(parsed, service.MockResources())
	require.NoError(t, err)

	_, ok := out.(*iotdbWriter)
	require.True(t, ok)

	// 注意：由于需要真实的 IoTDB 服务器连接才能测试写入功能，
	// 此处仅验证对象创建成功，实际的写入测试需要在集成环境中进行
}
