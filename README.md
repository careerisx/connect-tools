# Connect Tools

Connect Tools 是一个跨平台的 TCP、MQTT 调试工具。它由一个 Go 单文件服务程序和 React Web UI 组成，同时提供完整 CLI 与 MCP 接口，可供 Codex、Claude Code、OpenCode 等 AI 智能体读取连接状态、检查通信记录并执行受控的调试操作。

服务只监听本机回环地址 `127.0.0.1:8765`。Web UI 已嵌入 Go 可执行文件，运行时不依赖 Electron、系统 WebView、Node.js 或额外前端文件。

## 功能

| 功能 | 支持范围 |
| --- | --- |
| TCP 客户端 | 多连接、自动重连、文本/HEX/Base64/文件发送 |
| TCP 服务端 | 多监听会话、多客户端 Peer、广播或定向发送 |
| TCP 分帧 | 原始数据、固定长度、分隔符 |
| MQTT 客户端 | MQTT 3.1.1/5.0、QoS 0/1/2、Retain、订阅管理、自动重连 |
| MQTT 安全 | TLS、mTLS、系统凭据库保存密码和私钥口令 |
| 数据记录 | 收发方向、来源、会话、文本或 HEX 内容过滤，支持持续跟踪与等待 |
| 格式转换 | Text、HEX、Base64、转义序列、JSON、整数、浮点数、时间戳 |
| 校验和 | SUM8/16/32、CRC16 ARC/MODBUS/CCITT/X25、CRC32 IEEE/Castagnoli/Koopman |
| 使用方式 | Web UI、CLI、MCP |
| 操作系统 | Windows、macOS、Linux |

单次 TCP 发送或 MQTT 发布的最大负载为 16 MiB。

## 快速开始

已有对应平台的可执行文件时，直接启动服务：

```bash
connect-tools serve
```

Windows PowerShell 使用：

```powershell
.\connect-tools.exe serve
```

程序会在 `127.0.0.1:8765` 启动本地服务。浏览器直接访问 `http://127.0.0.1:8765/` 即可使用，无需登录。服务进程需要保持运行；在终端按 `Ctrl+C` 可以退出。如需启动时自动打开浏览器，可追加 `--open`。

启动时可通过 `--token` 指定 CLI 和 MCP 使用的访问令牌，也可与 `--open` 同时使用：

```powershell
.\connect-tools.exe serve --token "replace-with-your-random-token-at-least-32-chars" --open
```

示例中的占位内容必须替换为实际令牌。自定义令牌至少需要 32 个可打印 ASCII 字符，不允许空格、中文或控制字符；建议使用随机生成的高强度令牌。服务成功监听后，会原子保存到原有 `access-token` 文件，供其他 CLI 命令及后续启动使用。未指定 `--token` 时，继续读取已保存的令牌，不存在时自动生成。更换令牌前必须停止旧服务；已有服务运行或监听失败时，不会覆盖已保存的令牌。

此参数不会设置或修改 `CONNECT_TOOLS_TOKEN` 环境变量。命令行参数可能被终端历史记录或进程查看工具保存，请勿将真实令牌写入可共享的脚本或配置。

常用服务命令：

```bash
# 查看服务状态
connect-tools status

# 检查配置目录、系统凭据库、运行信息和服务健康状态
connect-tools doctor

# 让后台服务正常退出
connect-tools shutdown
```

如果服务已经运行，再次执行 `connect-tools serve --open` 会直接打开现有服务的 Web UI。

## 从源码构建

### 构建环境

| 工具 | 要求 |
| --- | --- |
| Go | 1.27 或更高版本 |
| Node.js | 可运行 Vite 7 的版本 |
| pnpm | 10.x |
| protoc | Protocol Buffers 编译器 |
| make | 执行项目构建目标 |

构建当前平台版本：

```bash
make build
```

产物位于：

```text
dist/connect-tools
```

交叉构建：

```bash
# Windows amd64
make build-windows

# macOS arm64、amd64
make build-macos

# Linux amd64、arm64
make build-linux
```

Windows 原生环境执行 Makefile 时，建议使用 Git Bash 或其他支持 POSIX 环境变量语法的终端。各构建目标会自动安装锁定版本的前端依赖、生成 Protobuf 代码、构建 Web UI，并将前端资源嵌入最终 Go 可执行文件。

开发时可分别执行：

```bash
make generate
pnpm --dir web build
go build -o dist/connect-tools ./cmd/connect-tools
```

## Web UI 使用方法

执行 `connect-tools serve` 并访问 `http://127.0.0.1:8765/` 后，可在 Web UI 中完成主要操作：

1. 在配置区域创建 TCP 客户端、TCP 服务端或 MQTT 配置。
2. 启动配置，创建一个独立运行会话。
3. 同时打开多个会话标签页，在不同连接之间切换。
4. 在发送区选择文本、HEX 或 Base64 格式并发送数据。
5. 在记录区查看入站、出站和内部事件，使用 Inspector 转换或检查负载。
6. 在设置区域启用或停用 MCP。

配置用于保存连接参数；会话表示一次正在运行的连接。一个配置可以启动多个相互独立的会话。TCP 服务端接受客户端后，每个客户端还会形成独立的 Peer 会话，可单独回复。

## CLI 基本约定

查看全部命令及任一子命令参数：

```bash
connect-tools --help
connect-tools profile create --help
connect-tools mqtt publish --help
```

大多数连接管理命令需要本地服务已经运行。建议在一个终端执行：

```bash
connect-tools serve
```

在另一个终端执行 CLI 操作。自动化脚本可添加全局参数 `--output json`；`records follow` 会连续输出一行一个 JSON 对象。

### 配置和会话

```bash
# 列出保存的配置
connect-tools profile list --output json

# 查看配置
connect-tools profile get <PROFILE_ID>

# 启动配置，可选指定会话显示名称
connect-tools session start <PROFILE_ID> --display-name "调试会话"

# 查看运行会话
connect-tools session list --output json
connect-tools session get <SESSION_ID>

# 停止会话
connect-tools session stop <SESSION_ID>

# 删除保存的配置
connect-tools profile delete <PROFILE_ID>
```

创建和启动命令返回的 JSON 中包含后续命令需要的 `profileId` 或 `sessionId`。

## TCP 调试

### TCP 客户端

下面的示例创建一个使用 `0D0A` 作为分隔符的 TCP 客户端：

```bash
connect-tools profile create \
  --kind tcp-client \
  --name echo-client \
  --host 127.0.0.1 \
  --port 9000 \
  --frame delimiter \
  --delimiter-hex 0D0A \
  --auto-reconnect
```

从返回结果取得 `PROFILE_ID`，然后启动并发送数据：

```bash
connect-tools session start <PROFILE_ID>

connect-tools tcp send <SESSION_ID> \
  --text "hello" \
  --append-delimiter-hex 0D0A
```

也可以发送其他格式。以下输入参数必须且只能选择一个：

```bash
connect-tools tcp send <SESSION_ID> --hex "01 02 03 FF"
connect-tools tcp send <SESSION_ID> --base64 "AQID/w=="
connect-tools tcp send <SESSION_ID> --file ./payload.bin
connect-tools tcp send <SESSION_ID> --file - < ./payload.bin
connect-tools tcp send <SESSION_ID> --text "中文" --charset gbk
```

### TCP 服务端

```bash
connect-tools profile create \
  --kind tcp-server \
  --name local-server \
  --bind-host 127.0.0.1 \
  --port 9000 \
  --max-connections 128 \
  --frame raw

connect-tools session start <PROFILE_ID>
```

查看已经接入的客户端：

```bash
connect-tools tcp peers <SERVER_SESSION_ID>
```

向服务端会话发送会广播给当前客户端；向 Peer 会话发送只回复指定客户端：

```bash
connect-tools tcp send <SERVER_SESSION_ID> --hex "01 02 03"
connect-tools tcp send <PEER_SESSION_ID> --text "reply"
```

### TCP 分帧参数

| 模式 | 参数 | 说明 |
| --- | --- | --- |
| 原始 | `--frame raw` | 将每次读取的数据直接作为记录 |
| 固定长度 | `--frame fixed --fixed-length 8` | 每 8 字节形成一帧 |
| 分隔符 | `--frame delimiter --delimiter-hex 0D0A` | 遇到指定字节序列形成一帧 |

分隔符模式可添加 `--include-delimiter`，使接收记录保留分隔符字节。

## MQTT 调试

### 创建客户端

不带密码的本地 Broker 示例：

```bash
connect-tools profile create \
  --kind mqtt \
  --name local-mqtt \
  --host 127.0.0.1 \
  --port 1883 \
  --mqtt-version 5 \
  --client-id connect-tools-cli \
  --clean-start \
  --auto-reconnect
```

需要用户名和密码时，通过标准输入传入密码，避免密码出现在命令历史中：

```bash
printf '%s\n' 'secret' | connect-tools profile create \
  --kind mqtt \
  --name secured-mqtt \
  --host broker.example.com \
  --port 1883 \
  --mqtt-version 5 \
  --username demo \
  --password-stdin
```

Windows PowerShell：

```powershell
'secret' | .\connect-tools.exe profile create `
  --kind mqtt `
  --name secured-mqtt `
  --host broker.example.com `
  --port 1883 `
  --mqtt-version 5 `
  --username demo `
  --password-stdin
```

启用 TLS 或 mTLS：

```bash
connect-tools profile create \
  --kind mqtt \
  --name tls-mqtt \
  --host broker.example.com \
  --port 8883 \
  --mqtt-version 5 \
  --tls \
  --ca-file ./ca.pem \
  --server-name broker.example.com

connect-tools profile create \
  --kind mqtt \
  --name mtls-mqtt \
  --host broker.example.com \
  --port 8883 \
  --mqtt-version 5 \
  --tls \
  --ca-file ./ca.pem \
  --client-cert ./client.crt \
  --client-key ./client.key
```

加密私钥的口令可通过 `--key-passphrase-stdin` 输入。一次创建命令只读取一个标准输入密钥，因此 `--password-stdin` 与 `--key-passphrase-stdin` 不应同时使用。

### 订阅和发布

```bash
connect-tools session start <PROFILE_ID>

connect-tools mqtt subscribe <SESSION_ID> \
  --filter 'devices/+/state' \
  --qos 1

connect-tools mqtt publish <SESSION_ID> \
  --topic 'devices/a/command' \
  --qos 1 \
  --text '{"on":true}'

connect-tools mqtt publish <SESSION_ID> \
  --topic 'devices/a/raw' \
  --qos 2 \
  --retain \
  --hex '01 02 FF'

connect-tools mqtt subscriptions <SESSION_ID>
connect-tools mqtt unsubscribe <SESSION_ID> 'devices/+/state'
```

MQTT 发布与 TCP 发送使用相同的 `--text`、`--hex`、`--base64`、`--file`、`--charset` 和 `--append-delimiter-hex` 输入参数。

## 通信记录

```bash
# 查看最近 100 条记录
connect-tools records list --limit 100

# 查看指定会话的入站记录
connect-tools records list \
  --session <SESSION_ID> \
  --direction inbound \
  --limit 100

# 过滤原始文本或字节内容
connect-tools records list --contains-text 'error'
connect-tools records list --contains-hex '01 03'

# 持续跟踪指定会话
connect-tools records follow --session <SESSION_ID> --output json

# 最多等待 30 秒，直到出现下一条匹配记录
connect-tools records wait \
  --session <SESSION_ID> \
  --after <SEQUENCE> \
  --timeout-ms 30000

# 清除指定会话记录，或清除全部记录
connect-tools records clear --session <SESSION_ID>
connect-tools records clear --all
```

可用过滤维度：

| 参数 | 可选值或含义 |
| --- | --- |
| `--session` | 会话 ID，可重复指定 |
| `--direction` | `inbound`、`outbound`、`internal` |
| `--actor` | `remote`、`user`、`cli`、`mcp`、`system` |
| `--contains-text` | 原始 UTF-8 负载包含的文本 |
| `--contains-hex` | 原始负载包含的 HEX 字节 |
| `--after` | 只返回指定序号之后的记录 |

通信记录保存在服务进程内存中，服务退出后不会保留。

## 数据转换和校验

格式转换不要求服务正在运行：

```bash
# 文本转 HEX
connect-tools convert \
  --data '你好' \
  --from text \
  --to hex \
  --charset utf-8

# HEX 转文本
connect-tools convert \
  --data '48656C6C6F' \
  --from hex \
  --to text

# 格式化 JSON
connect-tools convert \
  --data '{"name":"device","online":true}' \
  --from text \
  --to json-pretty

# Unix 秒时间戳转 RFC3339
connect-tools convert \
  --data '1700000000' \
  --from unix-s \
  --to rfc3339

# 32 位大端整数转 HEX
connect-tools convert \
  --data '1024' \
  --from number \
  --to integer \
  --bits 32 \
  --byte-order big
```

计算校验和：

```bash
connect-tools checksum --algorithm crc16-modbus --hex '01 03 00 00 00 02'
connect-tools checksum --algorithm crc32-ieee --text 'hello'
connect-tools checksum --algorithm sum8 --file ./payload.bin
```

支持的校验算法为 `sum8`、`sum16`、`sum32`、`crc16-arc`、`crc16-modbus`、`crc16-ccitt`、`crc16-x25`、`crc32-ieee`、`crc32-castagnoli`、`crc32-koopman`。

## MCP 接入 AI 智能体

MCP 默认关闭。启用后，AI 智能体可读取配置、会话和记录，也可以创建临时连接、发送 TCP 数据、发布或订阅 MQTT。MCP 只监听本机地址，并要求 Bearer Token。

### 1. 启动服务并启用 MCP

```bash
connect-tools serve
```

在另一个终端执行：

```bash
connect-tools mcp enable
connect-tools mcp status
```

MCP 地址为：

```text
http://127.0.0.1:8765/mcp
```

### 2. 设置访问令牌

macOS 或 Linux：

```bash
export CONNECT_TOOLS_TOKEN="$(connect-tools mcp token --show)"
```

Windows PowerShell：

```powershell
$env:CONNECT_TOOLS_TOKEN = (.\connect-tools.exe mcp token --show)
```

`mcp token` 必须显式添加 `--show` 才会输出令牌。如果服务通过 `serve --token` 启动，此命令读取的是已保存的自定义令牌，上述环境变量设置方式无需改变。更换令牌后，需要同步更新 MCP 客户端使用的令牌；已有客户端进程不会自动获得新的环境变量。

不要把令牌提交到仓库或写入可共享的配置文件。

### 3. 生成智能体配置

#### Codex

```bash
connect-tools mcp config --agent codex
```

把输出内容加入 Codex MCP 配置：

```toml
[mcp_servers.connect-tools]
url = "http://127.0.0.1:8765/mcp"
bearer_token_env_var = "CONNECT_TOOLS_TOKEN"
```

#### Claude Code

先设置 `CONNECT_TOOLS_TOKEN`，再执行生成的命令：

```bash
connect-tools mcp config --agent claude
```

当前生成格式为：

```bash
claude mcp add --transport http connect-tools http://127.0.0.1:8765/mcp \
  --header "Authorization: Bearer ${CONNECT_TOOLS_TOKEN}"
```

#### OpenCode

```bash
connect-tools mcp config --agent opencode
```

把输出的 `mcp.connect-tools` 配置合并到 OpenCode 配置文件：

```json
{
  "mcp": {
    "connect-tools": {
      "type": "remote",
      "url": "http://127.0.0.1:8765/mcp",
      "headers": {
        "Authorization": "Bearer {env:CONNECT_TOOLS_TOKEN}"
      }
    }
  }
}
```

AI 智能体使用 MCP 期间，`connect-tools serve` 必须保持运行。

### MCP 工具

| 类型 | 工具 |
| --- | --- |
| 只读 | `list_profiles`、`list_sessions`、`get_session`、`read_records`、`wait_for_record`、`get_diagnostics`、`convert_payload` |
| 操作 | `start_profile`、`create_tcp_client`、`create_tcp_server`、`create_mqtt_client`、`stop_session`、`send_tcp`、`publish_mqtt`、`subscribe_mqtt`、`unsubscribe_mqtt` |

建议让智能体先调用只读工具检查会话和通信记录，再执行发送、发布或订阅操作。调试结束后可关闭 MCP：

```bash
connect-tools mcp disable
```

## 配置和凭据

| 系统 | 配置目录 |
| --- | --- |
| Windows | `%APPDATA%\connect-tools` |
| macOS | `~/Library/Application Support/connect-tools` |
| Linux | `~/.config/connect-tools` |

主要文件：

| 文件 | 用途 |
| --- | --- |
| `config.json` | MCP 等应用设置 |
| `profiles.json` | TCP、MQTT 连接配置，不包含明文密码 |
| `runtime.json` | 当前服务地址、进程和实例信息 |
| `access-token` | MCP 接口的访问令牌 |

MQTT 密码和私钥口令优先保存在操作系统凭据库中：Windows Credential Manager、macOS Keychain、Linux Secret Service。如果系统凭据库不可用，程序会退回内存存储，服务退出后凭据会丢失。可使用 `connect-tools doctor` 检查当前凭据存储状态。

## 常见问题

### CLI 提示 `service is not running`

先在另一个终端启动服务：

```bash
connect-tools serve
```

然后运行 `connect-tools status` 确认状态。

### Web UI 无法打开

确认服务已经启动，然后直接访问 `http://127.0.0.1:8765/`。也可以使用下面的命令打开浏览器：

```bash
connect-tools serve --open
```

### MQTT 密码在重启后丢失

运行：

```bash
connect-tools doctor
```

如果凭据后端显示为 `memory`，需要先修复操作系统凭据库。Linux 桌面环境通常需要可用的 Secret Service。

### MCP 客户端返回未授权

重新设置当前终端中的 `CONNECT_TOOLS_TOKEN`，确认配置使用 `Authorization: Bearer <token>`，并检查 MCP 已启用：

```bash
connect-tools mcp status
```

### 端口被占用或连接失败

使用 `connect-tools doctor` 检查本地服务，再确认目标 TCP/MQTT 地址、端口、TLS Server Name 和证书路径。连接运行状态及错误原因也会写入通信记录，可通过 Web UI 或 `connect-tools records list` 查看。
