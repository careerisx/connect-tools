import { useEffect, useState } from "react";
import { Activity, Cable, Play, Plus, RefreshCw, Send, Square, Trash2 } from "lucide-react";

import { coreClient, profileClient } from "../../api/client";
import { FrameMode, SessionKind, SessionState } from "../../gen/connecttools/v1/session_pb";
import { useCoreStore } from "../../store/core";
import { useWorkbenchStore } from "../../store/workbench";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";

function NewProfile({ onDone }: { onDone(): void }) {
  const [open, setOpen] = useState(false);
  const [kind, setKind] = useState<"tcp-client" | "tcp-server" | "mqtt">("tcp-client");
  const [name, setName] = useState("");
  const [host, setHost] = useState("127.0.0.1");
  const [port, setPort] = useState("1883");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string>();

  const createProfile = async () => {
    setError(undefined);
    const numericPort = Number(port);
    if (!name.trim() || !Number.isInteger(numericPort) || numericPort < 1 || numericPort > 65535) {
      setError("Name and a valid port are required"); return;
    }
    const framing = { mode: FrameMode.RAW_READ, fixedLength: 0, delimiter: new Uint8Array(), includeDelimiter: false };
    const connection = kind === "tcp-client"
      ? { case: "tcpClient" as const, value: { host, port: numericPort, autoReconnect: true, keepAlive: true, noDelay: true, connectTimeoutMs: 5000, readTimeoutMs: 0, writeTimeoutMs: 5000, keepAlivePeriodMs: 30000, framing } }
      : kind === "tcp-server"
        ? { case: "tcpServer" as const, value: { bindHost: host, port: numericPort, maxConnections: 128, keepAlive: true, noDelay: true, framing } }
        : { case: "mqtt" as const, value: { version: 2, host, port: numericPort, clientId: "", username, passwordRef: "", keepAliveSeconds: 30, cleanStart: true, tls: { enabled: false, caFile: "", clientCertFile: "", clientKeyFile: "", serverName: "", privateKeyPassphraseRef: "" }, initialSubscriptions: [], reconnect: { enabled: true, initialDelayMs: 1000, maxDelayMs: 30000, multiplier: 2, jitterRatio: .2, maxAttempts: 0 } } };
    try {
      await profileClient.createProfile({ profile: { name, description: "", connection }, mqttPassword: kind === "mqtt" && password ? new TextEncoder().encode(password) : new Uint8Array() });
      setOpen(false); setName(""); setPassword(""); onDone();
    } catch (cause) { setError(cause instanceof Error ? cause.message : "Create failed"); }
  };

  if (!open) return <Button className="toolbar-button is-primary" variant="unstyled" size="unstyled" onClick={() => setOpen(true)}><Plus size={15}/>New connection</Button>;
  return <div className="profile-popover">
    <select value={kind} onChange={(event) => setKind(event.target.value as typeof kind)}><option value="tcp-client">TCP client</option><option value="tcp-server">TCP server</option><option value="mqtt">MQTT client</option></select>
    <input aria-label="Profile name" placeholder="Profile name" value={name} onChange={(event) => setName(event.target.value)}/>
    <input aria-label="Host" placeholder="Host" value={host} onChange={(event) => setHost(event.target.value)}/>
    <input aria-label="Port" placeholder="Port" value={port} onChange={(event) => setPort(event.target.value)}/>
    {kind === "mqtt" && <><input aria-label="Username" placeholder="Username" value={username} onChange={(event) => setUsername(event.target.value)}/><input aria-label="Password" type="password" placeholder="Password" value={password} onChange={(event) => setPassword(event.target.value)}/></>}
    {error && <span className="form-error">{error}</span>}
    <Button onClick={createProfile}>Save</Button><Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
  </div>;
}

export function AppShell() {
  const connected = useCoreStore((state) => state.connected);
  const status = useCoreStore((state) => state.status);
  const store = useWorkbenchStore();
  const [payload, setPayload] = useState("");
  const [topic, setTopic] = useState("connect-tools/debug");
  const [qos, setQos] = useState(0);
  const [retain, setRetain] = useState(false);
  const [subscription, setSubscription] = useState("#");
  useEffect(() => { void store.refresh(); const id = window.setInterval(() => void store.refresh(), 3000); return () => window.clearInterval(id); }, [store.refresh]);
  const selectedSession = store.sessions.find((value) => value.id === store.selectedSessionId);

  return <div className="app-shell">
    <header className="app-header">
      <div className="brand-block"><div className="brand-mark"><Cable size={17}/></div><div><div className="brand-name">Connect Tools</div><div className="brand-subtitle">TCP · MQTT · MCP workbench</div></div></div>
      <div className="toolbar"><span className={`connection-chip ${connected ? "is-online" : "is-offline"}`}><span className="status-dot"/>Core {connected ? "online" : "offline"}</span><Button variant="outline" onClick={() => void coreClient.setMCPEnabled({ enabled: !status?.mcpEnabled }).then(() => useCoreStore.getState().load())}>MCP {status?.mcpEnabled ? "enabled" : "disabled"}</Button><NewProfile onDone={() => void store.refresh()}/><Button className="icon-button" variant="unstyled" size="unstyled" onClick={() => void store.refresh()} aria-label="Refresh"><RefreshCw size={16}/></Button></div>
    </header>
    <main className="main-grid">
      <aside className="session-sidebar">
        <div className="sidebar-heading"><h2>Profiles</h2><span>{store.profiles.length}</span></div>
        {store.profiles.map((profile) => <div className="profile-row" key={profile.id}><div><strong>{profile.name}</strong><small>{profile.connection.case}</small></div><Button onClick={() => void store.start(profile.id)}><Play size={13}/>Start</Button></div>)}
        <div className="sidebar-heading"><h2>Sessions</h2><span>{store.sessions.length}</span></div>
        {store.sessions.filter((session) => !session.parentId).map((session) => <div key={session.id}>
          <button className={`session-row ${store.selectedSessionId === session.id ? "selected" : ""}`} onClick={() => void store.selectSession(session.id)}><span className="status-dot"/><span>{session.displayName}</span><Badge variant="outline">{SessionState[session.state]}</Badge></button>
          {store.sessions.filter((peer) => peer.parentId === session.id).map((peer) => <button className={`session-row peer ${store.selectedSessionId === peer.id ? "selected" : ""}`} key={peer.id} onClick={() => void store.selectSession(peer.id)}>↳ {peer.remoteAddress || peer.displayName}</button>)}
        </div>)}
      </aside>
      <section className="workspace">
        <header className="workspace-header"><div><p className="eyebrow">Session workspace</p><h1>{selectedSession?.displayName ?? "Select a session"}</h1><small>{selectedSession?.remoteAddress}</small></div>{selectedSession && <Button variant="outline" onClick={() => void store.stop(selectedSession.id)}><Square size={13}/>Stop</Button>}</header>
        {store.error && <div className="error-banner">{store.error}</div>}
        <div className="record-list">
          {store.records.length === 0 && <div className="timeline-empty"><Activity size={24}/><h2>No records</h2><p>Incoming and outgoing data will appear here.</p></div>}
          {store.records.map((record) => <button className={`record-row ${store.selectedRecord?.sequence === record.sequence ? "selected" : ""}`} key={record.sequence.toString()} onClick={() => store.selectRecord(record)}><span>#{record.sequence.toString()}</span><Badge variant="outline">{record.direction === 1 ? "IN" : record.direction === 2 ? "OUT" : "SYS"}</Badge><code>{new TextDecoder().decode(record.payload).slice(0, 160) || Array.from(record.payload).map((b) => b.toString(16).padStart(2, "0")).join(" ")}</code><span>{record.payload.length} B</span></button>)}
        </div>
        <div className="composer">
          {selectedSession?.kind === SessionKind.MQTT_CLIENT && <><div className="composer-toolbar"><input aria-label="MQTT topic" value={topic} onChange={(event) => setTopic(event.target.value)}/><select value={qos} onChange={(event) => setQos(Number(event.target.value))}><option value={0}>QoS 0</option><option value={1}>QoS 1</option><option value={2}>QoS 2</option></select><label><input type="checkbox" checked={retain} onChange={(event) => setRetain(event.target.checked)}/>Retain</label></div><div className="composer-toolbar"><input aria-label="MQTT subscription" value={subscription} onChange={(event) => setSubscription(event.target.value)}/><Button onClick={() => void store.subscribe(selectedSession.id, subscription, qos)}>Subscribe</Button><Button variant="outline" onClick={() => void store.unsubscribe(selectedSession.id, subscription)}>Unsubscribe</Button></div></>}
          <textarea aria-label="Payload" value={payload} onChange={(event) => setPayload(event.target.value)} placeholder="Enter UTF-8 payload"/>
          <div className="composer-footer"><span>{new TextEncoder().encode(payload).length} bytes</span><Button disabled={!selectedSession || store.loading} onClick={() => selectedSession && void store.send(selectedSession, payload, topic, qos, retain)}><Send size={14}/>Send</Button><Button variant="outline" disabled={!selectedSession} onClick={() => void store.clear()}><Trash2 size={14}/>Clear</Button></div>
        </div>
      </section>
      <aside className="inspector"><h2>Inspector</h2>{store.selectedRecord ? <><dl><dt>Sequence</dt><dd>{store.selectedRecord.sequence.toString()}</dd><dt>Session</dt><dd>{store.selectedRecord.sessionId}</dd><dt>Bytes</dt><dd>{store.selectedRecord.payload.length}</dd></dl><h3>HEX</h3><pre>{Array.from(store.selectedRecord.payload).map((b) => b.toString(16).padStart(2, "0").toUpperCase()).join(" ")}</pre><h3>Base64</h3><pre>{btoa(String.fromCharCode(...store.selectedRecord.payload))}</pre><h3>Metadata</h3><pre>{JSON.stringify(store.selectedRecord.protocolMetadata, null, 2)}</pre></> : <p>Select a record to inspect bytes and metadata.</p>}</aside>
    </main>
  </div>;
}
