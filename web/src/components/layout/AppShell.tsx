import * as Dialog from "@radix-ui/react-dialog";
import * as Tabs from "@radix-ui/react-tabs";
import { create } from "@bufbuild/protobuf";
import { useMemo, useState, type FormEvent } from "react";
import {
  Activity,
  ArrowDownLeft,
  ArrowUpRight,
  Cable,
  Clock3,
  Database,
  Inbox,
  Pencil,
  Play,
  Plus,
  RefreshCw,
  RotateCw,
  Send,
  Settings2,
  Square,
  Trash2,
  Users,
  X,
} from "lucide-react";

import { coreClient, profileClient } from "../../api/client";
import { MQTTProfileSchema, MQTTTLSConfigSchema, ReconnectPolicySchema } from "../../gen/connecttools/v1/mqtt_pb";
import { ProfileSchema, TCPClientProfileSchema, TCPServerProfileSchema, type Profile } from "../../gen/connecttools/v1/profile_pb";
import { FrameMode, FramingConfigSchema, PayloadEncoding, SessionKind, SessionState, type Session } from "../../gen/connecttools/v1/session_pb";
import { cn } from "../../lib/utils";
import { useCoreStore } from "../../store/core";
import { useWorkbenchStore } from "../../store/workbench";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";

const inspectorPreviewBytes = 64 << 10;
type MessageFormat = "text" | "hex" | "base64";
const messageFormatStorageKey = "connect-tools.message-format";
const sendEncodingStorageKey = "connect-tools.send-encoding";

/** 读取上次选择的消息显示格式。 */
function storedMessageFormat(): MessageFormat {
  try {
    const value = localStorage.getItem(messageFormatStorageKey);
    return value === "hex" || value === "base64" ? value : "text";
  } catch {
    return "text";
  }
}

/** 读取上次选择的发送编码。 */
function storedSendEncoding(): PayloadEncoding {
  try {
    const value = Number(localStorage.getItem(sendEncodingStorageKey));
    if (value === PayloadEncoding.HEX || value === PayloadEncoding.BASE64) return value;
  } catch {
    // 本地存储不可用时使用文本编码。
  }
  return PayloadEncoding.TEXT;
}

/** 保存无需同步到服务端的页面偏好。 */
function storePreference(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    // 浏览器禁用本地存储时保持当前页面内状态。
  }
}

/** 将配置连接类型转换为页面使用的中文名称。 */
function connectionKindLabel(kind: string | undefined): string {
  switch (kind) {
    case "tcpClient": return "TCP 客户端";
    case "tcpServer": return "TCP 服务端";
    case "mqtt": return "MQTT 客户端";
    default: return "未知类型";
  }
}

/** 将会话类型转换为页面使用的中文名称。 */
function sessionKindLabel(kind: SessionKind): string {
  switch (kind) {
    case SessionKind.TCP_CLIENT: return "TCP 客户端";
    case SessionKind.TCP_SERVER: return "TCP 服务端";
    case SessionKind.TCP_PEER: return "TCP 接入端";
    case SessionKind.MQTT_CLIENT: return "MQTT 客户端";
    default: return "未知会话";
  }
}

/** 将会话状态转换为页面使用的中文名称。 */
function sessionStateLabel(state: SessionState): string {
  switch (state) {
    case SessionState.STOPPED: return "已停止";
    case SessionState.STARTING: return "启动中";
    case SessionState.RUNNING: return "运行中";
    case SessionState.STOPPING: return "停止中";
    case SessionState.ERROR: return "错误";
    default: return "未知";
  }
}

/** 返回会话状态徽标对应的 Tailwind 样式。 */
function sessionStateBadgeClass(state: SessionState): string {
  if (state === SessionState.RUNNING) return "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300";
  if (state === SessionState.ERROR) return "border-red-200 bg-red-50 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-300";
  if (state === SessionState.STARTING || state === SessionState.STOPPING) return "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-300";
  return "border-slate-200 bg-slate-100 text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300";
}

/** 返回会话状态圆点对应的 Tailwind 样式。 */
function sessionStateDotClass(state: SessionState): string {
  if (state === SessionState.RUNNING) return "bg-emerald-500 ring-emerald-500/15";
  if (state === SessionState.ERROR) return "bg-red-500 ring-red-500/15";
  if (state === SessionState.STARTING || state === SessionState.STOPPING) return "bg-amber-500 ring-amber-500/15";
  return "bg-slate-400 ring-slate-400/15";
}

/** 将记录方向转换为简短的中文标签。 */
function recordDirectionLabel(direction: number): string {
  if (direction === 1) return "接收";
  if (direction === 2) return "发送";
  return "系统";
}

/** 安全地将大字节数组转换为 Base64，避免展开参数导致页面崩溃。 */
function bytesToBase64(bytes: Uint8Array): string {
  const chunkSize = 0x8000;
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
  }
  return btoa(binary);
}

/** 按指定格式返回列表使用的消息预览，避免转换完整的大消息。 */
function recordPreview(payload: Uint8Array, format: MessageFormat): string {
  const preview = payload.subarray(0, 256);
  if (format === "hex") {
    return Array.from(preview).map((byte) => byte.toString(16).padStart(2, "0").toUpperCase()).join(" ") || "（空内容）";
  }
  if (format === "base64") return bytesToBase64(preview) || "（空内容）";
  return new TextDecoder().decode(preview) || "（空内容）";
}

/** 按发送编码计算解码后的载荷字节数，格式无效时返回空值。 */
function payloadByteSize(data: string, encoding: PayloadEncoding): number | undefined {
  if (encoding === PayloadEncoding.TEXT) return new TextEncoder().encode(data).length;
  if (encoding === PayloadEncoding.HEX) {
    const compact = data.replace(/\s+/g, "");
    return compact.length % 2 === 0 && /^[0-9a-fA-F]*$/.test(compact) ? compact.length / 2 : undefined;
  }
  if (encoding === PayloadEncoding.BASE64) {
    const validBase64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
    if (!validBase64.test(data)) return undefined;
    try {
      return atob(data).length;
    } catch {
      return undefined;
    }
  }
  return undefined;
}

/** 返回连接配置用于展示的地址。 */
function profileEndpoint(profile: Profile): string {
  switch (profile.connection.case) {
    case "tcpClient": return `${profile.connection.value.host}:${profile.connection.value.port}`;
    case "tcpServer": return `${profile.connection.value.bindHost}:${profile.connection.value.port}`;
    case "mqtt": return `${profile.connection.value.host}:${profile.connection.value.port}`;
    default: return "未配置地址";
  }
}

/** 生成连接配置的稳定匹配键，避免相同连接被重复保存。 */
function profileConnectionKey(profile: Profile): string {
  switch (profile.connection.case) {
    case "tcpClient":
      return `tcp-client|${profile.connection.value.host.trim().toLowerCase()}|${profile.connection.value.port}`;
    case "tcpServer":
      return `tcp-server|${profile.connection.value.bindHost.trim().toLowerCase()}|${profile.connection.value.port}`;
    case "mqtt":
      return `mqtt|${profile.connection.value.host.trim().toLowerCase()}|${profile.connection.value.port}|${profile.connection.value.username.trim()}`;
    default:
      return `unknown|${profile.id}`;
  }
}

/** 将时间戳转换为会话管理列表使用的中文时间。 */
function formatTimestamp(timestamp: Session["updatedAt"]): string {
  if (!timestamp) return "暂无记录";
  const date = new Date(Number(timestamp.seconds) * 1000 + timestamp.nanos / 1_000_000);
  if (Number.isNaN(date.getTime())) return "暂无记录";
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "short", timeStyle: "short" }).format(date);
}

/** 提供新建并立即启动 TCP 或 MQTT 会话的表单。 */
function NewSessionDialog() {
  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [kind, setKind] = useState<"tcp-client" | "tcp-server" | "mqtt">("tcp-client");
  const [name, setName] = useState("");
  const [host, setHost] = useState("127.0.0.1");
  const [port, setPort] = useState("9000");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string>();

  /** 同步弹窗开关状态，并在重新打开时清理上一次的错误提示。 */
  const changeOpen = (nextOpen: boolean) => {
    setOpen(nextOpen);
    if (nextOpen) setError(undefined);
  };

  /** 切换连接类型并为新类型设置常用默认端口。 */
  const changeKind = (nextKind: typeof kind) => {
    setKind(nextKind);
    setPort(nextKind === "mqtt" ? "1883" : "9000");
  };

  /** 复用或保存历史配置，然后启动对应会话。 */
  const createSession = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(undefined);
    const numericPort = Number(port);
    if (!host.trim()) {
      setError(kind === "tcp-server" ? "请输入监听地址" : "请输入主机地址");
      return;
    }
    if (!Number.isInteger(numericPort) || numericPort < 1 || numericPort > 65535) {
      setError("端口必须是 1 至 65535 之间的整数");
      return;
    }

    const framing = {
      mode: FrameMode.RAW_READ,
      fixedLength: 0,
      delimiter: new Uint8Array(),
      includeDelimiter: false,
    };
    const normalizedHost = host.trim();
    const connection = kind === "tcp-client"
      ? { case: "tcpClient" as const, value: { host: normalizedHost, port: numericPort, autoReconnect: true, keepAlive: true, noDelay: true, connectTimeoutMs: 5000, readTimeoutMs: 0, writeTimeoutMs: 5000, keepAlivePeriodMs: 30000, framing } }
      : kind === "tcp-server"
        ? { case: "tcpServer" as const, value: { bindHost: normalizedHost, port: numericPort, maxConnections: 128, keepAlive: true, noDelay: true, framing } }
        : { case: "mqtt" as const, value: { version: 2, host: normalizedHost, port: numericPort, clientId: "", username: username.trim(), passwordRef: "", keepAliveSeconds: 30, cleanStart: true, tls: { enabled: false, caFile: "", clientCertFile: "", clientKeyFile: "", serverName: "", privateKeyPassphraseRef: "" }, initialSubscriptions: [], reconnect: { enabled: true, initialDelayMs: 1000, maxDelayMs: 30000, multiplier: 2, jitterRatio: .2, maxAttempts: 0 } } };
    const generatedName = `${kind === "tcp-client" ? "TCP 客户端" : kind === "tcp-server" ? "TCP 服务端" : "MQTT 客户端"} ${normalizedHost}:${numericPort}`;
    const draftKey = kind === "mqtt"
      ? `${kind}|${normalizedHost.toLowerCase()}|${numericPort}|${username.trim()}`
      : `${kind}|${normalizedHost.toLowerCase()}|${numericPort}`;

    setSaving(true);
    try {
      const workbench = useWorkbenchStore.getState();
      const existing = workbench.profiles.find((profile) => profileConnectionKey(profile) === draftKey);
      let profileId = existing?.id;
      if (existing && kind === "mqtt" && password) {
        await profileClient.updateProfile({
          profile: existing,
          mqttPassword: new TextEncoder().encode(password),
        });
      }
      if (!profileId) {
        const response = await profileClient.createProfile({
          profile: { name: generatedName, description: "", connection },
          mqttPassword: kind === "mqtt" && password
            ? new TextEncoder().encode(password)
            : new Uint8Array(),
        });
        profileId = response.profile?.id;
      }
      if (!profileId) throw new Error("未能保存会话配置");
      const started = await workbench.start(profileId, name.trim() || undefined);
      if (!started) throw new Error(useWorkbenchStore.getState().error ?? "连接失败");
      await workbench.refresh();
      setOpen(false);
      setName("");
      setPassword("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "创建失败");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog.Root open={open} onOpenChange={changeOpen}>
      <Dialog.Trigger asChild>
        <Button className="w-full"><Plus size={16}/>新增会话</Button>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-slate-950/50 backdrop-blur-sm" />
        <Dialog.Content className="fixed top-1/2 left-1/2 z-50 max-h-[calc(100vh-2.5rem)] w-[min(520px,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 overflow-y-auto rounded-2xl border border-slate-200 bg-white p-5 font-sans text-slate-900 shadow-2xl outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" aria-describedby="new-profile-description">
          <div className="flex items-start justify-between gap-4 border-b border-slate-200 pb-4 dark:border-slate-700">
            <div>
              <Dialog.Title className="text-lg font-bold">新增会话</Dialog.Title>
              <Dialog.Description className="mt-1 text-xs leading-5 text-slate-500 dark:text-slate-400" id="new-profile-description">
                选择连接方式并填写参数，连接信息会自动保存到会话管理。
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <Button variant="ghost" size="icon" aria-label="关闭"><X size={18}/></Button>
            </Dialog.Close>
          </div>

          <form className="mt-5 grid grid-cols-1 gap-4 sm:grid-cols-2" onSubmit={createSession}>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 sm:col-span-2 dark:text-slate-300">
              <span>连接类型</span>
              <select className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-blue-500 focus:ring-3 focus:ring-blue-500/15 dark:border-slate-600 dark:bg-slate-800" value={kind} onChange={(event) => changeKind(event.target.value as typeof kind)}>
                <option value="tcp-client">TCP 客户端</option>
                <option value="tcp-server">TCP 服务端</option>
                <option value="mqtt">MQTT 客户端</option>
              </select>
            </label>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 sm:col-span-2 dark:text-slate-300">
              <span>会话名称（选填）</span>
              <input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none placeholder:text-slate-400 focus:border-blue-500 focus:ring-3 focus:ring-blue-500/15 dark:border-slate-600 dark:bg-slate-800" autoFocus placeholder="不填写则按连接地址自动生成" value={name} onChange={(event) => setName(event.target.value)}/>
            </label>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300">
              <span>{kind === "tcp-server" ? "监听地址" : "主机地址"}</span>
              <input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none placeholder:text-slate-400 focus:border-blue-500 focus:ring-3 focus:ring-blue-500/15 dark:border-slate-600 dark:bg-slate-800" placeholder="127.0.0.1" value={host} onChange={(event) => setHost(event.target.value)}/>
            </label>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300">
              <span>端口</span>
              <input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none placeholder:text-slate-400 focus:border-blue-500 focus:ring-3 focus:ring-blue-500/15 dark:border-slate-600 dark:bg-slate-800" inputMode="numeric" placeholder="1883" value={port} onChange={(event) => setPort(event.target.value)}/>
            </label>
            {kind === "mqtt" ? (
              <>
                <label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300">
                  <span>用户名</span>
                  <input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none placeholder:text-slate-400 focus:border-blue-500 focus:ring-3 focus:ring-blue-500/15 dark:border-slate-600 dark:bg-slate-800" autoComplete="username" placeholder="可选" value={username} onChange={(event) => setUsername(event.target.value)}/>
                </label>
                <label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300">
                  <span>密码</span>
                  <input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none placeholder:text-slate-400 focus:border-blue-500 focus:ring-3 focus:ring-blue-500/15 dark:border-slate-600 dark:bg-slate-800" autoComplete="current-password" type="password" placeholder="可选" value={password} onChange={(event) => setPassword(event.target.value)}/>
                </label>
              </>
            ) : null}
            {error ? <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 sm:col-span-2 dark:border-red-800 dark:bg-red-950 dark:text-red-300" role="alert">{error}</div> : null}
            <div className="flex justify-end gap-2 border-t border-slate-200 pt-4 sm:col-span-2 dark:border-slate-700">
              <Dialog.Close asChild><Button variant="outline" disabled={saving}>取消</Button></Dialog.Close>
              <Button type="submit" disabled={saving}>{saving ? "正在连接…" : "创建并连接"}</Button>
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

/** 提供已保存会话配置的修改表单。 */
function EditProfileDialog({ profile }: { profile: Profile }) {
  const initialKind = profile.connection.case === "tcpServer" ? "tcp-server" : profile.connection.case === "mqtt" ? "mqtt" : "tcp-client";
  const initialHost = profile.connection.case === "tcpServer" ? profile.connection.value.bindHost : profile.connection.case === "mqtt" || profile.connection.case === "tcpClient" ? profile.connection.value.host : "127.0.0.1";
  const initialPort = profile.connection.case === undefined ? "9000" : String(profile.connection.value.port);
  const initialUsername = profile.connection.case === "mqtt" ? profile.connection.value.username : "";
  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [kind, setKind] = useState<"tcp-client" | "tcp-server" | "mqtt">(initialKind);
  const [name, setName] = useState(profile.name);
  const [host, setHost] = useState(initialHost);
  const [port, setPort] = useState(initialPort);
  const [username, setUsername] = useState(initialUsername);
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string>();

  /** 打开弹窗时从最新配置恢复表单。 */
  const changeOpen = (nextOpen: boolean) => {
    setOpen(nextOpen);
    if (!nextOpen) return;
    setKind(initialKind);
    setName(profile.name);
    setHost(initialHost);
    setPort(initialPort);
    setUsername(initialUsername);
    setPassword("");
    setError(undefined);
  };

  /** 校验并完整更新会话配置，空密码表示保留原凭据。 */
  const updateProfile = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(undefined);
    const numericPort = Number(port);
    const normalizedHost = host.trim();
    if (!normalizedHost) {
      setError(kind === "tcp-server" ? "请输入监听地址" : "请输入主机地址");
      return;
    }
    if (!Number.isInteger(numericPort) || numericPort < 1 || numericPort > 65535) {
      setError("端口必须是 1 至 65535 之间的整数");
      return;
    }

    const framing = create(FramingConfigSchema, { mode: FrameMode.RAW_READ, fixedLength: 0, delimiter: new Uint8Array(), includeDelimiter: false });
    let connection: Profile["connection"];
    if (kind === "tcp-client") {
      connection = {
        case: "tcpClient",
        value: create(TCPClientProfileSchema, profile.connection.case === "tcpClient"
          ? { ...profile.connection.value, host: normalizedHost, port: numericPort }
          : { host: normalizedHost, port: numericPort, autoReconnect: true, keepAlive: true, noDelay: true, connectTimeoutMs: 5000, readTimeoutMs: 0, writeTimeoutMs: 5000, keepAlivePeriodMs: 30000, framing }),
      };
    } else if (kind === "tcp-server") {
      connection = {
        case: "tcpServer",
        value: create(TCPServerProfileSchema, profile.connection.case === "tcpServer"
          ? { ...profile.connection.value, bindHost: normalizedHost, port: numericPort }
          : { bindHost: normalizedHost, port: numericPort, maxConnections: 128, keepAlive: true, noDelay: true, framing }),
      };
    } else {
      connection = {
        case: "mqtt",
        value: create(MQTTProfileSchema, profile.connection.case === "mqtt"
          ? { ...profile.connection.value, host: normalizedHost, port: numericPort, username: username.trim() }
          : {
              version: 2,
              host: normalizedHost,
              port: numericPort,
              clientId: "",
              username: username.trim(),
              passwordRef: "",
              keepAliveSeconds: 30,
              cleanStart: true,
              tls: create(MQTTTLSConfigSchema, { enabled: false, caFile: "", clientCertFile: "", clientKeyFile: "", serverName: "", privateKeyPassphraseRef: "" }),
              initialSubscriptions: [],
              reconnect: create(ReconnectPolicySchema, { enabled: true, initialDelayMs: 1000, maxDelayMs: 30000, multiplier: 2, jitterRatio: .2, maxAttempts: 0 }),
            }),
      };
    }
    const generatedName = `${kind === "tcp-client" ? "TCP 客户端" : kind === "tcp-server" ? "TCP 服务端" : "MQTT 客户端"} ${normalizedHost}:${numericPort}`;

    setSaving(true);
    try {
      await profileClient.updateProfile({
        profile: create(ProfileSchema, { ...profile, name: name.trim() || generatedName, connection }),
        mqttPassword: kind === "mqtt" && password ? new TextEncoder().encode(password) : new Uint8Array(),
      });
      await useWorkbenchStore.getState().refresh();
      setOpen(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "修改失败");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog.Root open={open} onOpenChange={changeOpen}>
      <Dialog.Trigger asChild><Button className="flex-1" variant="outline" size="sm"><Pencil size={13}/>修改</Button></Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-slate-950/50 backdrop-blur-sm" />
        <Dialog.Content className="fixed top-1/2 left-1/2 z-50 max-h-[calc(100vh-2.5rem)] w-[min(520px,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 overflow-y-auto rounded-2xl border border-slate-200 bg-white p-5 font-sans text-slate-900 shadow-2xl outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" aria-describedby="edit-profile-description">
          <div className="flex items-start justify-between gap-4 border-b border-slate-200 pb-4 dark:border-slate-700">
            <div><Dialog.Title className="text-lg font-bold">修改会话</Dialog.Title><Dialog.Description className="mt-1 text-xs leading-5 text-slate-500 dark:text-slate-400" id="edit-profile-description">更新后将用于下一次连接，当前活动连接不受影响。</Dialog.Description></div>
            <Dialog.Close asChild><Button variant="ghost" size="icon" aria-label="关闭"><X size={18}/></Button></Dialog.Close>
          </div>
          <form className="mt-5 grid grid-cols-1 gap-4 sm:grid-cols-2" onSubmit={updateProfile}>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 sm:col-span-2 dark:text-slate-300"><span>连接类型</span><select className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-blue-500 dark:border-slate-600 dark:bg-slate-800" value={kind} onChange={(event) => setKind(event.target.value as typeof kind)}><option value="tcp-client">TCP 客户端</option><option value="tcp-server">TCP 服务端</option><option value="mqtt">MQTT 客户端</option></select></label>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 sm:col-span-2 dark:text-slate-300"><span>会话名称</span><input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-blue-500 dark:border-slate-600 dark:bg-slate-800" value={name} onChange={(event) => setName(event.target.value)}/></label>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300"><span>{kind === "tcp-server" ? "监听地址" : "主机地址"}</span><input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-blue-500 dark:border-slate-600 dark:bg-slate-800" value={host} onChange={(event) => setHost(event.target.value)}/></label>
            <label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300"><span>端口</span><input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-blue-500 dark:border-slate-600 dark:bg-slate-800" inputMode="numeric" value={port} onChange={(event) => setPort(event.target.value)}/></label>
            {kind === "mqtt" ? <><label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300"><span>用户名</span><input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-blue-500 dark:border-slate-600 dark:bg-slate-800" value={username} onChange={(event) => setUsername(event.target.value)}/></label><label className="grid gap-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300"><span>新密码（选填）</span><input className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-blue-500 dark:border-slate-600 dark:bg-slate-800" type="password" placeholder="留空则保持原密码" value={password} onChange={(event) => setPassword(event.target.value)}/></label></> : null}
            {error ? <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 sm:col-span-2 dark:border-red-800 dark:bg-red-950 dark:text-red-300" role="alert">{error}</div> : null}
            <div className="flex justify-end gap-2 border-t border-slate-200 pt-4 sm:col-span-2 dark:border-slate-700"><Dialog.Close asChild><Button variant="outline" disabled={saving}>取消</Button></Dialog.Close><Button type="submit" disabled={saving}>{saving ? "正在保存…" : "保存修改"}</Button></div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

/** 确认并删除一条已保存会话配置。 */
function DeleteProfileDialog({ profile }: { profile: Profile }) {
  const [open, setOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [error, setError] = useState<string>();

  /** 删除配置并刷新会话管理列表。 */
  const deleteProfile = async () => {
    setDeleting(true);
    setError(undefined);
    try {
      await profileClient.deleteProfile({ profileId: profile.id });
      await useWorkbenchStore.getState().refresh();
      setOpen(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "删除失败");
    } finally {
      setDeleting(false);
    }
  };

  return (
    <Dialog.Root open={open} onOpenChange={(nextOpen) => { setOpen(nextOpen); if (nextOpen) setError(undefined); }}>
      <Dialog.Trigger asChild><Button className="flex-1 text-red-600 hover:bg-red-50 hover:text-red-700 dark:text-red-300 dark:hover:bg-red-950" variant="ghost" size="sm"><Trash2 size={13}/>删除</Button></Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-slate-950/50 backdrop-blur-sm" />
        <Dialog.Content className="fixed top-1/2 left-1/2 z-50 w-[min(420px,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-slate-200 bg-white p-5 font-sans text-slate-900 shadow-2xl outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100" aria-describedby="delete-profile-description">
          <Dialog.Title className="text-lg font-bold">删除会话配置</Dialog.Title>
          <Dialog.Description className="mt-2 text-sm leading-6 text-slate-500 dark:text-slate-400" id="delete-profile-description">确定删除“{profile.name}”吗？删除后无法通过会话管理快速重新连接。</Dialog.Description>
          {error ? <div className="mt-4 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-300" role="alert">{error}</div> : null}
          <div className="mt-5 flex justify-end gap-2"><Dialog.Close asChild><Button variant="outline" disabled={deleting}>取消</Button></Dialog.Close><Button className="bg-red-600 hover:bg-red-700 dark:bg-red-500 dark:hover:bg-red-400" disabled={deleting} onClick={() => void deleteProfile()}>{deleting ? "正在删除…" : "确认删除"}</Button></div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

/** 展示活动会话、实时记录、记录详情和会话管理。 */
export function AppShell() {
  const connected = useCoreStore((state) => state.connected);
  const status = useCoreStore((state) => state.status);
  const store = useWorkbenchStore();
  const [payload, setPayload] = useState("");
  const [sendEncoding, setSendEncoding] = useState(storedSendEncoding);
  const [topic, setTopic] = useState("connect-tools/debug");
  const [qos, setQos] = useState(0);
  const [retain, setRetain] = useState(false);
  const [subscription, setSubscription] = useState("#");
  const [tcpTargetId, setTcpTargetId] = useState("all");
  const [messageFormat, setMessageFormat] = useState<MessageFormat>(storedMessageFormat);
  const selectedSession = store.sessions.find((value) => value.id === store.selectedSessionId);
  const selectedRecord = store.selectedRecord;
  const payloadSize = useMemo(() => payloadByteSize(payload, sendEncoding), [payload, sendEncoding]);
  const sessionGroups = useMemo(() => {
    const roots: Session[] = [];
    const peers = new Map<string, Session[]>();
    for (const session of store.sessions) {
      const active = session.state === SessionState.STARTING || session.state === SessionState.RUNNING || session.state === SessionState.STOPPING;
      if (!active) continue;
      if (!session.parentId) {
        roots.push(session);
        continue;
      }
      const group = peers.get(session.parentId) ?? [];
      group.push(session);
      peers.set(session.parentId, group);
    }
    return { roots, peers };
  }, [store.sessions]);
  const selectedServerId = selectedSession?.kind === SessionKind.TCP_SERVER
    ? selectedSession.id
    : selectedSession?.kind === SessionKind.TCP_PEER
      ? selectedSession.parentId
      : undefined;
  const tcpPeers = selectedServerId
    ? (sessionGroups.peers.get(selectedServerId) ?? []).filter((peer) => peer.state === SessionState.RUNNING)
    : [];
  const effectiveTargetId = tcpTargetId === "all" || tcpPeers.some((peer) => peer.id === tcpTargetId) ? tcpTargetId : "all";
  const sendTarget = selectedSession?.kind === SessionKind.TCP_SERVER && effectiveTargetId !== "all"
    ? tcpPeers.find((peer) => peer.id === effectiveTargetId)
    : selectedSession;
  const canSend = sendTarget !== undefined && sendTarget.state === SessionState.RUNNING && (
    sendTarget.kind === SessionKind.TCP_CLIENT ||
    sendTarget.kind === SessionKind.TCP_PEER ||
    sendTarget.kind === SessionKind.MQTT_CLIENT ||
    (sendTarget.kind === SessionKind.TCP_SERVER && tcpPeers.length > 0)
  );
  const managedProfiles = useMemo(() => Array.from(
    new Map(store.profiles.map((profile) => [profile.id, profile])).values(),
  ), [store.profiles]);
  const lastSessionByProfile = useMemo(() => {
    const result = new Map<string, Session>();
    for (const session of store.sessions) {
      if (!session.profileId || session.parentId) continue;
      const previous = result.get(session.profileId);
      const currentTime = session.updatedAt ? Number(session.updatedAt.seconds) : 0;
      const previousTime = previous?.updatedAt ? Number(previous.updatedAt.seconds) : 0;
      if (!previous || currentTime >= previousTime) result.set(session.profileId, session);
    }
    return result;
  }, [store.sessions]);
  const inspectedRecord = useMemo(() => {
    if (!selectedRecord) return undefined;
    const preview = selectedRecord.payload.subarray(0, inspectorPreviewBytes);
    return {
      text: new TextDecoder().decode(preview) || "（空内容）",
      hex: Array.from(preview).map((byte) => byte.toString(16).padStart(2, "0").toUpperCase()).join(" ") || "（空内容）",
      base64: bytesToBase64(preview) || "（空内容）",
      metadata: Object.keys(selectedRecord.protocolMetadata).length > 0
        ? JSON.stringify(selectedRecord.protocolMetadata, null, 2)
        : "（无元数据）",
      truncated: selectedRecord.payload.length > preview.length,
    };
  }, [selectedRecord]);

  /** 更新并持久化消息列表的显示格式。 */
  const changeMessageFormat = (format: MessageFormat) => {
    setMessageFormat(format);
    storePreference(messageFormatStorageKey, format);
  };

  /** 更新并持久化发送载荷的编码格式。 */
  const changeSendEncoding = (encoding: PayloadEncoding) => {
    setSendEncoding(encoding);
    storePreference(sendEncodingStorageKey, String(encoding));
  };

  return (
    <div className="grid min-h-dvh grid-rows-[56px_auto] overflow-x-hidden bg-slate-100 font-sans text-sm text-slate-900 min-[1021px]:fixed min-[1021px]:inset-0 min-[1021px]:min-h-[520px] min-[1021px]:grid-rows-[64px_minmax(0,1fr)] min-[1021px]:overflow-hidden dark:bg-slate-950 dark:text-slate-100">
      <header className="relative z-10 flex items-center justify-between gap-2 border-b border-slate-200 bg-white/95 px-3 shadow-sm backdrop-blur-md sm:gap-6 sm:px-4 dark:border-slate-800 dark:bg-slate-900/95">
        <div className="flex min-w-0 items-center gap-2.5 sm:min-w-60 sm:gap-3">
          <div className="grid size-9 shrink-0 place-items-center rounded-xl border border-blue-200 bg-blue-50 text-blue-600 dark:border-blue-800 dark:bg-blue-950 dark:text-blue-300"><Cable size={19}/></div>
          <div><div className="text-[15px] font-bold leading-tight">连接工具</div><div className="mt-0.5 text-[11px] text-slate-400 max-[1020px]:hidden">TCP · MQTT · MCP 调试工作台</div></div>
        </div>
        <div className="flex items-center justify-end gap-2">
          <span className="mr-1 inline-flex items-center gap-2 whitespace-nowrap text-xs font-semibold text-slate-500 dark:text-slate-400"><span className={cn("size-2 rounded-full ring-3", connected ? "bg-emerald-500 ring-emerald-500/15" : "bg-red-500 ring-red-500/15")}/><span className="max-[430px]:hidden">核心服务{connected ? "在线" : "离线"}</span></span>
          <Button variant="outline" size="sm" onClick={() => void coreClient.setMCPEnabled({ enabled: !status?.mcpEnabled }).then(() => useCoreStore.getState().load())}><span className="max-[430px]:hidden">MCP </span>{status?.mcpEnabled ? "已启用" : "已禁用"}</Button>
          <Button variant="ghost" size="icon" onClick={() => void store.refresh()} aria-label="刷新数据"><RefreshCw size={17}/></Button>
        </div>
      </header>

      <main className="grid min-h-0 grid-cols-1 bg-white min-[768px]:grid-cols-[240px_minmax(0,1fr)] min-[1021px]:grid-cols-[250px_minmax(460px,1fr)_300px] min-[1241px]:grid-cols-[280px_minmax(500px,1fr)_340px] min-[1021px]:overflow-hidden dark:bg-slate-900">
        <aside className="min-h-0 min-w-0 max-h-[42dvh] overflow-y-auto border-b border-slate-200 bg-slate-50 min-[768px]:max-h-none min-[768px]:border-r min-[768px]:border-b-0 dark:border-slate-800 dark:bg-slate-950">
          <div className="px-3 pt-3.5 pb-1"><NewSessionDialog/></div>

          <section className="px-3 pt-3 pb-3">
            <div className="mb-2 flex min-h-7 items-center justify-between px-1"><h2 className="text-[11px] font-bold tracking-[0.08em] text-slate-500 dark:text-slate-400">活动会话</h2><span className="min-w-5.5 rounded-full bg-slate-200 px-2 py-0.5 text-center text-[11px] text-slate-500 dark:bg-slate-800 dark:text-slate-400">{sessionGroups.roots.length}</span></div>
            {sessionGroups.roots.length === 0 ? (
              <div className="grid grid-cols-[24px_minmax(0,1fr)] items-start gap-2 rounded-xl border border-dashed border-slate-300 p-3 text-slate-400 dark:border-slate-700"><Activity size={18}/><p className="m-0 text-xs leading-5">暂无活动会话，点击上方按钮创建或从会话管理重新连接。</p></div>
            ) : sessionGroups.roots.map((session) => (
              <div className="mt-1" key={session.id}>
                <div className={cn("flex items-center overflow-hidden rounded-xl border transition-colors", store.selectedSessionId === session.id ? "border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-950" : "border-transparent hover:bg-slate-100 dark:hover:bg-slate-800")}>
                  <button className="flex min-h-20.5 min-w-0 flex-1 items-center gap-2.5 bg-transparent px-2 py-2 text-left" onClick={() => void store.selectSession(session.id)}>
                    <span className={cn("size-2 shrink-0 rounded-full ring-3", sessionStateDotClass(session.state))}/>
                    <span className="grid min-w-0 flex-1 gap-0.5">
                      <strong className="overflow-hidden text-ellipsis whitespace-nowrap text-[13px] font-semibold">{session.displayName}</strong>
                      <small className="overflow-hidden text-ellipsis whitespace-nowrap text-[11px] text-slate-400">{sessionKindLabel(session.kind)}</small>
                      <span className="mt-0.5 grid min-w-0 gap-0.5 text-[10px] leading-snug text-slate-400">
                        {session.kind === SessionKind.TCP_SERVER ? (
                          <>
                            <span className="overflow-hidden text-ellipsis whitespace-nowrap" title={session.localAddress || "监听地址待建立"}>监听：{session.localAddress || "地址待建立"}</span>
                            <span className="overflow-hidden text-ellipsis whitespace-nowrap">客户端：{(sessionGroups.peers.get(session.id) ?? []).length}</span>
                          </>
                        ) : (
                          <>
                            <span className="overflow-hidden text-ellipsis whitespace-nowrap" title={session.localAddress || "本地地址待建立"}>本地：{session.localAddress || "待建立"}</span>
                            <span className="overflow-hidden text-ellipsis whitespace-nowrap" title={session.remoteAddress || "远端地址待建立"}>远端：{session.remoteAddress || "待建立"}</span>
                          </>
                        )}
                      </span>
                    </span>
                    <Badge className={sessionStateBadgeClass(session.state)}>{sessionStateLabel(session.state)}</Badge>
                  </button>
                  <Button className="mr-1.5 size-8 text-slate-400 hover:bg-red-50 hover:text-red-600 dark:hover:bg-red-950 dark:hover:text-red-300" variant="ghost" size="icon" disabled={session.state === SessionState.STOPPING || store.loading} onClick={() => void store.stop(session.id)} aria-label={`停止 ${session.displayName}`} title="停止会话"><Square size={13}/></Button>
                </div>
              </div>
            ))}
          </section>
        </aside>

        <section className="grid min-h-[calc(100dvh-56px)] min-w-0 grid-rows-[auto_minmax(240px,1fr)_auto] bg-[radial-gradient(circle_at_1px_1px,rgb(203_213_225_/_0.45)_1px,transparent_0)] [background-size:24px_24px] min-[768px]:min-h-[calc(100dvh-64px)] min-[1021px]:min-h-0 dark:bg-[radial-gradient(circle_at_1px_1px,rgb(51_65_85_/_0.45)_1px,transparent_0)]">
          <header className="flex min-h-18.5 flex-wrap items-center justify-between gap-2 border-b border-slate-200 bg-white/95 px-3 py-3 backdrop-blur-md sm:gap-5 sm:px-4.5 dark:border-slate-800 dark:bg-slate-900/95">
            <div className="min-w-0">
              <h1 className="text-[17px] font-bold">{selectedSession?.displayName ?? "请选择会话"}</h1>
              <small className="mt-1 block text-[11px] text-slate-400">{selectedSession ? `${sessionKindLabel(selectedSession.kind)} · ${selectedSession.remoteAddress || "等待连接"}` : "从左侧选择一个正在运行的会话"}</small>
            </div>
            <div className="inline-flex shrink-0 gap-1 rounded-lg border border-slate-200 bg-slate-50 p-1 dark:border-slate-700 dark:bg-slate-800" role="group" aria-label="消息显示格式">
              {([
                ["text", "文本"],
                ["hex", "十六进制"],
                ["base64", "Base64"],
              ] as const).map(([value, label]) => (
                <button key={value} className={cn("h-7.5 rounded-md px-2.5 text-[11px] font-semibold whitespace-nowrap text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white", messageFormat === value && "bg-white text-blue-600 shadow-sm ring-1 ring-blue-500/15 dark:bg-slate-700 dark:text-blue-300")} aria-pressed={messageFormat === value} onClick={() => changeMessageFormat(value)}>{label}</button>
              ))}
            </div>
          </header>
          {store.error ? <div className="border-b border-red-200 bg-red-50 px-3.5 py-2 text-xs text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-300" role="alert">{store.error}</div> : null}

          <div className="min-h-0 overflow-auto py-2">
            {store.records.length === 0 ? (
              <div className="flex h-full min-h-60 flex-col items-center justify-center px-6 text-center text-slate-400">
                <div className="mb-3 grid size-13 place-items-center rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800"><Activity size={28}/></div>
                <h2 className="m-0 text-base font-bold text-slate-700 dark:text-slate-200">{selectedSession ? "等待数据" : "尚未选择会话"}</h2>
                <p className="mt-2 text-xs leading-5">{selectedSession ? "接收和发送的数据会实时显示在这里。" : "选择左侧会话后，可以查看记录并发送调试数据。"}</p>
              </div>
            ) : store.records.map((record) => (
              <button className={cn("grid min-h-12 w-full grid-cols-[74px_minmax(0,1fr)_64px] items-center gap-2 border-b border-slate-200/70 bg-white/90 px-3 py-2 text-left hover:bg-blue-50/70 sm:grid-cols-[72px_86px_minmax(0,1fr)_76px] sm:gap-2.5 sm:px-4 dark:border-slate-800 dark:bg-slate-900/90 dark:hover:bg-blue-950/50", store.selectedRecord?.sequence === record.sequence && "bg-blue-50 shadow-[inset_3px_0_0_#2563eb] dark:bg-blue-950/60")} key={record.sequence.toString()} onClick={() => store.selectRecord(record)}>
                <span className="hidden text-[11px] text-slate-400 sm:block">#{record.sequence.toString()}</span>
                <Badge className={record.direction === 1 ? "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300" : record.direction === 2 ? "border-blue-200 bg-blue-50 text-blue-700 dark:border-blue-800 dark:bg-blue-950 dark:text-blue-300" : "border-slate-200 bg-slate-100 text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"}>
                  {record.direction === 1
                    ? <ArrowDownLeft size={12}/>
                    : record.direction === 2
                      ? <ArrowUpRight size={12}/>
                      : <Activity size={12}/>} {recordDirectionLabel(record.direction)}
                </Badge>
                <code className="min-w-0 overflow-hidden text-ellipsis whitespace-nowrap font-mono text-xs text-slate-600 dark:text-slate-300">{recordPreview(record.payload, messageFormat)}</code>
                <span className="text-right text-[11px] text-slate-400">{record.payload.length} 字节</span>
              </button>
            ))}
          </div>

          <div className="mx-2 mb-2 flex min-h-44 max-h-none flex-col overflow-hidden rounded-xl border border-slate-300 bg-white shadow-lg sm:mx-3.5 sm:mb-3.5 min-[1021px]:max-h-72 dark:border-slate-700 dark:bg-slate-900">
            {selectedSession?.kind === SessionKind.MQTT_CLIENT ? (
              <div className="grid grid-cols-2 gap-2 border-b border-slate-200 bg-slate-50 p-2.5 sm:grid-cols-[minmax(160px,1fr)_110px_108px] dark:border-slate-800 dark:bg-slate-950">
                <label className="col-span-2 grid gap-1 text-[10px] font-semibold text-slate-500 sm:col-span-1 dark:text-slate-400"><span>发布主题</span><input className="h-8 min-w-0 rounded-lg border border-slate-300 bg-white px-2 text-xs outline-none focus:border-blue-500 dark:border-slate-700 dark:bg-slate-900" value={topic} onChange={(event) => setTopic(event.target.value)}/></label>
                <label className="grid gap-1 text-[10px] font-semibold text-slate-500 dark:text-slate-400"><span>服务质量</span><select className="h-8 min-w-0 rounded-lg border border-slate-300 bg-white px-2 text-xs outline-none focus:border-blue-500 dark:border-slate-700 dark:bg-slate-900" value={qos} onChange={(event) => setQos(Number(event.target.value))}><option value={0}>QoS 0</option><option value={1}>QoS 1</option><option value={2}>QoS 2</option></select></label>
                <label className="flex items-center justify-center gap-2 pt-3.5 text-[10px] font-semibold text-slate-500 dark:text-slate-400"><input className="size-4" type="checkbox" checked={retain} onChange={(event) => setRetain(event.target.checked)}/><span>保留消息</span></label>
                <label className="col-span-2 grid gap-1 text-[10px] font-semibold text-slate-500 sm:col-span-1 dark:text-slate-400"><span>订阅主题</span><input className="h-8 min-w-0 rounded-lg border border-slate-300 bg-white px-2 text-xs outline-none focus:border-blue-500 dark:border-slate-700 dark:bg-slate-900" value={subscription} onChange={(event) => setSubscription(event.target.value)}/></label>
                <Button variant="outline" size="sm" disabled={!canSend || !subscription.trim() || store.loading} onClick={() => void store.subscribe(selectedSession.id, subscription, qos)}>订阅</Button>
                <Button variant="ghost" size="sm" disabled={!canSend || !subscription.trim() || store.loading} onClick={() => void store.unsubscribe(selectedSession.id, subscription)}>取消订阅</Button>
              </div>
            ) : null}
            {!canSend ? (
              <div className="flex items-center gap-2 border-b border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300">
                <Inbox size={17}/>
                <span>{selectedSession?.kind === SessionKind.TCP_SERVER ? "当前没有可发送的已接入客户端。" : "请选择一个运行中的可发送会话。"}</span>
              </div>
            ) : null}
            <textarea
              className="w-full min-h-20 flex-1 resize-none border-0 bg-transparent px-3.5 py-3 font-mono text-xs leading-5 outline-none placeholder:text-slate-400 disabled:bg-slate-50 dark:disabled:bg-slate-950"
              disabled={!canSend}
              aria-label="消息内容"
              value={payload}
              onChange={(event) => setPayload(event.target.value)}
              placeholder={sendEncoding === PayloadEncoding.HEX
                ? "输入十六进制内容，例如：48 65 6C 6C 6F"
                : sendEncoding === PayloadEncoding.BASE64
                  ? "输入 Base64 内容，例如：SGVsbG8="
                  : "输入要发送的 UTF-8 消息内容…"}
            />
            <div className="flex min-h-12 flex-col items-stretch gap-2 border-t border-slate-200 px-2.5 py-2 text-[11px] text-slate-400 sm:flex-row sm:items-center sm:justify-between sm:gap-3 dark:border-slate-800">
              <div className="flex min-w-0 flex-wrap items-center gap-2 sm:gap-3.5">
                <label className="flex min-w-0 items-center gap-1.5 font-semibold text-slate-500 dark:text-slate-400">
                  <span>发送格式</span>
                  <select className="h-8 rounded-lg border border-slate-300 bg-white px-2 text-[11px] text-slate-700 outline-none focus:border-blue-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200" value={sendEncoding} onChange={(event) => changeSendEncoding(Number(event.target.value) as PayloadEncoding)}>
                    <option value={PayloadEncoding.TEXT}>文本</option>
                    <option value={PayloadEncoding.HEX}>十六进制</option>
                    <option value={PayloadEncoding.BASE64}>Base64</option>
                  </select>
                </label>
                <span>{payloadSize === undefined ? "格式待校验" : `${payloadSize} 字节`}</span>
                {selectedSession?.kind === SessionKind.TCP_SERVER ? (
                  <label className="flex w-full min-w-0 items-center gap-1.5 font-semibold text-slate-500 sm:w-auto dark:text-slate-400">
                    <Users size={14}/><span>发送到</span>
                    <select className="h-8 min-w-0 flex-1 rounded-lg border border-slate-300 bg-white px-2 text-[11px] text-slate-700 outline-none focus:border-blue-500 disabled:cursor-not-allowed disabled:opacity-50 sm:w-[min(240px,28vw)] sm:flex-none dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200" value={effectiveTargetId} onChange={(event) => setTcpTargetId(event.target.value)} disabled={tcpPeers.length === 0}>
                      <option value="all">全部客户端（{tcpPeers.length}）</option>
                      {tcpPeers.map((peer) => <option value={peer.id} key={peer.id}>{peer.remoteAddress || peer.displayName}</option>)}
                    </select>
                  </label>
                ) : null}
              </div>
              <div className="flex items-center justify-end gap-2">
                <Button className="max-sm:flex-1" variant="ghost" size="sm" disabled={!selectedSession} onClick={() => void store.clear()}><Trash2 size={14}/>清空记录</Button>
                <Button className="max-sm:flex-1" size="sm" disabled={!canSend || store.loading || payloadSize === undefined} onClick={() => sendTarget && void store.send(sendTarget, payload, sendEncoding, topic, qos, retain)}><Send size={14}/>发送</Button>
              </div>
            </div>
          </div>
        </section>

        <Tabs.Root className="flex min-h-[480px] min-w-0 flex-col overflow-hidden border-t border-slate-200 bg-slate-50 min-[768px]:col-span-2 min-[1021px]:col-span-1 min-[1021px]:min-h-0 min-[1021px]:border-t-0 min-[1021px]:border-l dark:border-slate-800 dark:bg-slate-950" defaultValue="details">
          <Tabs.List className="grid shrink-0 grid-cols-3 border-b border-slate-200 bg-white p-2 dark:border-slate-800 dark:bg-slate-900" aria-label="右侧信息栏">
            <Tabs.Trigger className="flex h-9 min-w-0 items-center justify-center gap-1.5 rounded-lg text-xs font-semibold text-slate-500 hover:bg-slate-100 data-[state=active]:bg-blue-50 data-[state=active]:text-blue-600 dark:text-slate-400 dark:hover:bg-slate-800 dark:data-[state=active]:bg-blue-950 dark:data-[state=active]:text-blue-300" value="details"><Database size={15}/>记录详情</Tabs.Trigger>
            <Tabs.Trigger className="flex h-9 min-w-0 items-center justify-center gap-1.5 rounded-lg text-xs font-semibold text-slate-500 hover:bg-slate-100 data-[state=active]:bg-blue-50 data-[state=active]:text-blue-600 dark:text-slate-400 dark:hover:bg-slate-800 dark:data-[state=active]:bg-blue-950 dark:data-[state=active]:text-blue-300" value="clients"><Users size={15}/>活动客户端</Tabs.Trigger>
            <Tabs.Trigger className="flex h-9 min-w-0 items-center justify-center gap-1.5 rounded-lg text-xs font-semibold text-slate-500 hover:bg-slate-100 data-[state=active]:bg-blue-50 data-[state=active]:text-blue-600 dark:text-slate-400 dark:hover:bg-slate-800 dark:data-[state=active]:bg-blue-950 dark:data-[state=active]:text-blue-300" value="management"><Settings2 size={15}/>会话管理</Tabs.Trigger>
          </Tabs.List>
          <Tabs.Content className="min-h-0 flex-1 overflow-y-auto outline-none" value="details">
            {selectedRecord && inspectedRecord ? (
              <div className="grid gap-3.5 p-3.5">
                <header className="-mx-3.5 -mt-3.5 flex min-h-18.5 items-center justify-between border-b border-slate-200 bg-white px-4 py-3 text-slate-500 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-400"><div><p className="mb-1 text-[10px] font-bold tracking-[0.09em] text-blue-600 dark:text-blue-400">记录详情</p><h2 className="text-[15px] font-bold text-slate-900 dark:text-slate-100">数据检查器</h2></div><Database size={18}/></header>
                <dl className="grid grid-cols-2 gap-px overflow-hidden rounded-xl border border-slate-200 bg-slate-200 dark:border-slate-700 dark:bg-slate-700">
                  <div className="min-w-0 bg-white p-2.5 dark:bg-slate-900"><dt className="mb-1 text-[10px] text-slate-400">序号</dt><dd className="m-0 overflow-hidden text-ellipsis whitespace-nowrap text-xs font-semibold">#{selectedRecord.sequence.toString()}</dd></div>
                  <div className="min-w-0 bg-white p-2.5 dark:bg-slate-900"><dt className="mb-1 text-[10px] text-slate-400">方向</dt><dd className="m-0 overflow-hidden text-ellipsis whitespace-nowrap text-xs font-semibold">{recordDirectionLabel(selectedRecord.direction)}</dd></div>
                  <div className="min-w-0 bg-white p-2.5 dark:bg-slate-900"><dt className="mb-1 text-[10px] text-slate-400">字节数</dt><dd className="m-0 overflow-hidden text-ellipsis whitespace-nowrap text-xs font-semibold">{selectedRecord.payload.length}</dd></div>
                  <div className="min-w-0 bg-white p-2.5 dark:bg-slate-900"><dt className="mb-1 text-[10px] text-slate-400">会话 ID</dt><dd className="m-0 overflow-hidden text-ellipsis whitespace-nowrap text-xs font-semibold" title={selectedRecord.sessionId}>{selectedRecord.sessionId}</dd></div>
                </dl>
                {inspectedRecord.truncated ? <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-[11px] text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300">内容较大，仅预览前 64 KiB。</div> : null}
                {[["文本", inspectedRecord.text], ["十六进制", inspectedRecord.hex], ["Base64", inspectedRecord.base64], ["元数据", inspectedRecord.metadata]].map(([title, content]) => (
                  <section className="overflow-hidden rounded-xl border border-slate-200 bg-white dark:border-slate-700 dark:bg-slate-900" key={title}>
                    <h3 className="border-b border-slate-200 bg-slate-50 px-3 py-2 text-[11px] font-bold text-slate-500 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-400">{title}</h3>
                    <pre className="max-h-48 overflow-auto whitespace-pre-wrap [overflow-wrap:anywhere] p-3 font-mono text-[11px] leading-5 text-slate-600 dark:text-slate-300">{content}</pre>
                  </section>
                ))}
              </div>
            ) : (
              <div className="flex h-full min-h-60 flex-col items-center justify-center px-6 text-center text-slate-400"><Database size={28}/><h3 className="mt-3 text-sm font-bold text-slate-600 dark:text-slate-300">选择一条记录</h3><p className="mt-2 text-[11px] leading-5">这里会显示文本、十六进制、Base64 和协议元数据。</p></div>
            )}
          </Tabs.Content>
          <Tabs.Content className="min-h-0 flex-1 overflow-y-auto p-3.5 outline-none" value="clients">
            <header className="mb-3 flex items-center justify-between"><div><p className="mb-1 text-[10px] font-bold tracking-[0.09em] text-blue-600 dark:text-blue-400">TCP 服务端</p><h2 className="text-[15px] font-bold">活动客户端</h2></div><span className="min-w-6 rounded-full bg-slate-200 px-2 py-0.5 text-center text-[11px] text-slate-500 dark:bg-slate-800 dark:text-slate-400">{tcpPeers.length}</span></header>
            {!selectedServerId ? (
              <div className="flex min-h-60 flex-col items-center justify-center px-6 text-center text-slate-400"><Users size={28}/><h3 className="mt-3 text-sm font-bold text-slate-600 dark:text-slate-300">请选择 TCP 服务端</h3><p className="mt-2 text-[11px] leading-5">选择左侧的 TCP 服务端后，这里会显示已接入客户端。</p></div>
            ) : tcpPeers.length === 0 ? (
              <div className="flex min-h-60 flex-col items-center justify-center px-6 text-center text-slate-400"><Users size={28}/><h3 className="mt-3 text-sm font-bold text-slate-600 dark:text-slate-300">暂无活动客户端</h3><p className="mt-2 text-[11px] leading-5">客户端接入后会实时显示在这里。</p></div>
            ) : (
              <div className="grid gap-2">
                {tcpPeers.map((peer) => (
                  <button className={cn("flex w-full min-w-0 items-center gap-2.5 rounded-xl border bg-white p-2.5 text-left transition-colors dark:bg-slate-900", store.selectedSessionId === peer.id ? "border-blue-300 bg-blue-50 dark:border-blue-700 dark:bg-blue-950" : "border-slate-200 hover:border-blue-300 hover:bg-blue-50 dark:border-slate-700 dark:hover:border-blue-700 dark:hover:bg-blue-950")} key={peer.id} onClick={() => void store.selectSession(peer.id)}>
                    <span className="grid size-8 shrink-0 place-items-center rounded-lg bg-emerald-50 text-emerald-600 dark:bg-emerald-950 dark:text-emerald-300"><Users size={16}/></span>
                    <span className="grid min-w-0 flex-1 gap-0.5"><strong className="overflow-hidden text-ellipsis whitespace-nowrap text-[13px]">{peer.remoteAddress || peer.displayName}</strong><small className="overflow-hidden text-ellipsis whitespace-nowrap text-[10px] text-slate-400">{peer.localAddress || "已连接到服务端"}</small></span>
                    <Badge className="border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300">运行中</Badge>
                  </button>
                ))}
              </div>
            )}
          </Tabs.Content>
          <Tabs.Content className="min-h-0 flex-1 overflow-y-auto p-3.5 outline-none" value="management">
            <header className="mb-3 flex items-center justify-between"><div><p className="mb-1 text-[10px] font-bold tracking-[0.09em] text-blue-600 dark:text-blue-400">已保存配置</p><h2 className="text-[15px] font-bold">会话管理</h2></div><span className="min-w-6 rounded-full bg-slate-200 px-2 py-0.5 text-center text-[11px] text-slate-500 dark:bg-slate-800 dark:text-slate-400">{managedProfiles.length}</span></header>
            {managedProfiles.length === 0 ? (
              <div className="flex min-h-60 flex-col items-center justify-center px-6 text-center text-slate-400"><Settings2 size={28}/><h3 className="mt-3 text-sm font-bold text-slate-600 dark:text-slate-300">暂无会话配置</h3><p className="mt-2 text-[11px] leading-5">创建会话后，可以在这里修改、删除或重新连接。</p></div>
            ) : (
              <div className="grid gap-2.5">
                {managedProfiles.map((profile) => {
                  const lastSession = lastSessionByProfile.get(profile.id);
                  const activeSession = sessionGroups.roots.find((session) => session.profileId === profile.id);
                  return (
                    <article className="grid gap-2.5 rounded-xl border border-slate-200 bg-white p-3 shadow-sm dark:border-slate-700 dark:bg-slate-900" key={profile.id}>
                      <div className="flex items-center justify-between gap-2.5">
                        <div className="grid min-w-0 gap-0.5"><strong className="overflow-hidden text-ellipsis whitespace-nowrap text-[13px]">{profile.name}</strong><small className="overflow-hidden text-ellipsis whitespace-nowrap text-[10px] text-slate-400">{connectionKindLabel(profile.connection.case)}</small></div>
                        {lastSession ? <Badge className={activeSession ? "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300" : "border-slate-200 bg-slate-100 text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"}>{activeSession ? "活动中" : sessionStateLabel(lastSession.state)}</Badge> : null}
                      </div>
                      <code className="overflow-hidden text-ellipsis whitespace-nowrap rounded-lg bg-slate-50 px-2.5 py-2 text-[11px] text-slate-600 dark:bg-slate-950 dark:text-slate-300">{profileEndpoint(profile)}</code>
                      <div className="flex items-center gap-1.5 text-[10px] text-slate-400"><Clock3 size={13}/><span>{lastSession ? `最近连接：${formatTimestamp(lastSession.updatedAt)}` : "尚未连接"}</span></div>
                      <div className="flex gap-2">
                        <Button className="flex-1" variant="outline" size="sm" disabled={store.loading} onClick={() => void store.start(profile.id)}>
                          {activeSession ? <Play size={13}/> : <RotateCw size={13}/>}{activeSession ? "切换" : "连接"}
                        </Button>
                        <EditProfileDialog profile={profile}/>
                        <DeleteProfileDialog profile={profile}/>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </Tabs.Content>
        </Tabs.Root>
      </main>
    </div>
  );
}
