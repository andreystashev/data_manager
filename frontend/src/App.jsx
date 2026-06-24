import { useState, useMemo, useEffect, useCallback, useRef } from "react";

const API_BASE = "";

const PARAMS = [
  { key: "lidars",  label: "Lidars",    color: "#f59e0b" },
  { key: "cameras", label: "Cameras",    color: "#06b6d4" },
  { key: "radars",  label: "Radars",    color: "#f97316" },
  { key: "other",   label: "Other", color: "#a855f7" },
];

const STATUS_CFG = {
  pending:     { label: "Pending",   color: "#6b7280", glow: false },
  downloading: { label: "Downloading", color: "#f59e0b", glow: true  },
  done:        { label: "Done",      color: "#10b981", glow: false },
  stopped:     { label: "Stopped",  color: "#06b6d4", glow: false },
  error:       { label: "Error",      color: "#ef4444", glow: false },
};

const themes = {
  dark: {
    bg:            "#0f0a1a",
    surface:       "#1a1428",
    border:        "#2d2540",
    text:          "#e8e0f0",
    textSecondary: "#9d8db5",
    accent:        "#f59e0b",
    selectedBg:    "#1f1528",
  },
  light: {
    bg:            "#f8f6f0",
    surface:       "#ffffff",
    border:        "#e0dcd0",
    text:          "#2d2a20",
    textSecondary: "#7a7568",
    accent:        "#0d9488",
    selectedBg:    "#ecfdf5",
  },
};

const DEBUG_PANEL_H = 280;
const DEBUG_BAR_H   = 29;
const LOG_COLORS = { info: "#06b6d4", warn: "#f59e0b", error: "#ef4444", ok: "#10b981" };

// =============================================================================
// Hooks
// =============================================================================

function useDebounce(value, delay = 400) {
  const [d, setD] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setD(value), delay);
    return () => clearTimeout(t);
  }, [value, delay]);
  return d;
}

function useBags(search, page, refreshKey) {
  const [state, setState] = useState({ datasets: [], total: 0, pages: 1, loading: true, error: null });
  const ctrl = useRef();
  useEffect(() => {
    if (ctrl.current) ctrl.current.abort();
    ctrl.current = new AbortController();
    setState(s => ({ ...s, loading: true, error: null }));
    fetch(`${API_BASE}/api/datasets?search=${encodeURIComponent(search)}&page=${page}`, { signal: ctrl.current.signal })
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(data => setState({ ...data, loading: false, error: null }))
      .catch(err => { if (err.name !== "AbortError") setState(s => ({ ...s, loading: false, error: String(err) })); });
    return () => ctrl.current.abort();
  }, [search, page, refreshKey]);
  return state;
}

function useQueue(onFetchError) {
  const [queue, setQueue] = useState([]);
  const poll = useCallback(() => {
    const ctrl = new AbortController();
    const timeout = setTimeout(() => ctrl.abort(), 3000);
    fetch(`${API_BASE}/api/queue`, { signal: ctrl.signal })
      .then(r => { clearTimeout(timeout); return r.json(); })
      .then(setQueue)
      .catch(() => { clearTimeout(timeout); onFetchError?.(); });
  }, [onFetchError]);
  useEffect(() => { poll(); const t = setInterval(poll, 2000); return () => clearInterval(t); }, [poll]);

  const addToQueue = useCallback((items) =>
    fetch(`${API_BASE}/api/queue`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ items }) })
      .then(r => r.json()).then(poll),
  [poll]);

  const stopItem = useCallback((id) =>
    fetch(`${API_BASE}/api/queue/${id}/stop`, { method: "POST" }).then(poll), [poll]);

  const resumeItem = useCallback((id) =>
    fetch(`${API_BASE}/api/queue/${id}/resume`, { method: "POST" }).then(poll), [poll]);

  const removeItem = useCallback((id) =>
    fetch(`${API_BASE}/api/queue/${id}`, { method: "DELETE" }).then(poll), [poll]);

  const clearDone = useCallback(() =>
    fetch(`${API_BASE}/api/queue/done`, { method: "DELETE" }).then(poll), [poll]);

  return { queue, addToQueue, stopItem, resumeItem, removeItem, clearDone };
}

function useHealth() {
  const [online, setOnline] = useState(null); // null = unknown, true/false
  const onlineRef = useRef(null);
  const checkRef = useRef(null);

  useEffect(() => {
    let mounted = true;
    let timer = null;

    const check = () => {
      const ctrl = new AbortController();
      const timeout = setTimeout(() => ctrl.abort(), 2500);
      fetch(`${API_BASE}/api/health`, { signal: ctrl.signal })
        .then(r => {
          clearTimeout(timeout);
          if (!mounted) return;
          if (!onlineRef.current && r.ok) schedule(8000); // was offline, now online — relax
          onlineRef.current = r.ok;
          setOnline(r.ok);
        })
        .catch(() => {
          clearTimeout(timeout);
          if (!mounted) return;
          if (onlineRef.current !== false) schedule(3000); // just went offline — check faster
          onlineRef.current = false;
          setOnline(false);
        });
    };
    checkRef.current = check;

    const schedule = (ms) => { clearInterval(timer); timer = setInterval(check, ms); };

    // Browser network events for instant reaction
    const goOffline = () => { if (mounted) { onlineRef.current = false; setOnline(false); check(); } };
    const goOnline  = () => { if (mounted) check(); };
    window.addEventListener("offline", goOffline);
    window.addEventListener("online", goOnline);

    check();
    schedule(5000); // default interval

    return () => { mounted = false; clearInterval(timer); window.removeEventListener("offline", goOffline); window.removeEventListener("online", goOnline); };
  }, []);

  return { online, recheck: () => checkRef.current?.() };
}

function useLog() {
  const [logs, setLogs] = useState([]);
  const addLog = useCallback((level, msg, data = null) => {
    const now = new Date();
    const ts = now.toLocaleTimeString("ru-RU", { hour12: false }) + "." + String(now.getMilliseconds()).padStart(3, "0");
    setLogs(prev => [{ id: now.getTime() + Math.random(), ts, level, msg, data }, ...prev].slice(0, 500));
  }, []);
  const clearLog = useCallback(() => setLogs([]), []);
  return { logs, addLog, clearLog };
}

// =============================================================================
// UI Components
// =============================================================================

function Skeleton({ theme }) {
  const t = themes[theme];
  return (
    <div style={{ background: t.surface, border: `1px solid ${t.border}`, borderRadius: 6, padding: "14px 18px", display: "flex", gap: 16, alignItems: "center" }}>
      <div style={{ width: 18, height: 18, background: t.border, borderRadius: 4, flexShrink: 0, animation: "pulse 1.4s infinite" }} />
      <div style={{ flex: 1 }}>
        <div style={{ height: 12, background: t.border, borderRadius: 3, marginBottom: 7, width: "70%", animation: "pulse 1.4s infinite" }} />
        <div style={{ height: 10, background: t.border, borderRadius: 3, width: "35%", animation: "pulse 1.4s 0.2s infinite" }} />
      </div>
      <div style={{ width: 52, height: 20, background: t.border, borderRadius: 3, animation: "pulse 1.4s infinite" }} />
    </div>
  );
}

function Checkbox({ checked, color, size = 15, onClick }) {
  return (
    <div onClick={onClick} style={{
      width: size, height: size,
      border: `1px solid ${checked ? color : "#3d3550"}`,
      borderRadius: 3,
      background: checked ? color + "28" : "transparent",
      display: "flex", alignItems: "center", justifyContent: "center",
      flexShrink: 0, transition: "all 0.15s", cursor: onClick ? "pointer" : "default",
    }}>
      {checked && <span style={{ color, fontSize: size - 4, lineHeight: 1 }}>&#10003;</span>}
    </div>
  );
}

function TrimSlider({ startSec, duration, trimStart, trimEnd, onTrimChange, theme, metaLoading }) {
  const t = themes[theme];

  const fmtTime = (sec) => {
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = Math.floor(sec % 60);
    return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}` : `${m}:${String(s).padStart(2, "0")}`;
  };

  const fmtTimestamp = (epochSec) => {
    if (!epochSec) return "--:--:--";
    return new Date(epochSec * 1000).toLocaleTimeString("ru-RU", { hour12: false });
  };

  const fmtDate = (epochSec) => {
    if (!epochSec) return "";
    return new Date(epochSec * 1000).toLocaleDateString("ru-RU");
  };

  const pctL = duration > 0 ? (trimStart / duration * 100) : 0;
  const pctR = duration > 0 ? (trimEnd / duration * 100) : 100;

  return (
    <div style={{ padding: "10px 0" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 7 }}>
        {metaLoading ? (
          <span style={{ fontSize: 11, color: t.textSecondary }}>Loading metadata...</span>
        ) : startSec ? (<>
          <span style={{ fontSize: 11, color: t.textSecondary }}>{fmtDate(startSec)} {fmtTimestamp(startSec)}</span>
          <span style={{ fontSize: 11, color: t.textSecondary }}>Duration: {fmtTime(duration)}</span>
          <span style={{ fontSize: 11, color: t.textSecondary }}>{fmtTimestamp(startSec + duration)}</span>
        </>) : (
          <span style={{ fontSize: 11, color: t.textSecondary }}>Duration: {fmtTime(duration)}</span>
        )}
      </div>
      <div style={{ position: "relative", height: 22, background: t.border, borderRadius: 4, overflow: "hidden" }}>
        <style>{`
          .trim-range { -webkit-appearance: none; appearance: none; background: transparent; pointer-events: none; }
          .trim-range::-webkit-slider-thumb { -webkit-appearance: none; width: 18px; height: 22px; background: transparent; pointer-events: auto; cursor: pointer; }
          .trim-range::-moz-range-thumb { width: 18px; height: 22px; background: transparent; border: none; pointer-events: auto; cursor: pointer; }
        `}</style>
        <div style={{ position: "absolute", left: `${pctL}%`, width: `${pctR - pctL}%`, height: "100%", background: "rgba(245, 158, 11, 0.2)", borderRadius: 3 }} />
        <input type="range" className="trim-range" min={0} max={duration} value={trimStart} step={1}
          onChange={e => onTrimChange(Math.min(Number(e.target.value), trimEnd - 1), trimEnd)}
          style={{ position: "absolute", inset: 0, width: "100%", height: "100%", margin: 0, zIndex: 2 }}
        />
        <input type="range" className="trim-range" min={0} max={duration} value={trimEnd} step={1}
          onChange={e => onTrimChange(trimStart, Math.max(Number(e.target.value), trimStart + 1))}
          style={{ position: "absolute", inset: 0, width: "100%", height: "100%", margin: 0, zIndex: 3 }}
        />
        <div style={{ position: "absolute", left: `${pctL}%`, top: 0, width: 3, height: "100%", background: "#f59e0b", borderRadius: 1, pointerEvents: "none" }} />
        <div style={{ position: "absolute", left: `${pctR}%`, top: 0, width: 3, height: "100%", background: "#f59e0b", borderRadius: 1, transform: "translateX(-3px)", pointerEvents: "none" }} />
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 5 }}>
        <span style={{ fontSize: 11, color: "#f59e0b" }}>
          {startSec ? fmtTimestamp(startSec + trimStart) : fmtTime(trimStart)}
        </span>
        <span style={{ fontSize: 11, color: t.textSecondary }}>{fmtTime(trimEnd - trimStart)} selected</span>
        <span style={{ fontSize: 11, color: "#f59e0b" }}>
          {startSec ? fmtTimestamp(startSec + trimEnd) : fmtTime(trimEnd)}
        </span>
      </div>
    </div>
  );
}

// =============================================================================
// DatasetRow
// =============================================================================

function DatasetRow({ dataset, selParams, onToggle, onParamToggle, onMetaToggle, onSetAll, onTrimChange, onRequestTrim, onScanBag, theme }) {
  const t = themes[theme];
  const isSelected = !!selParams;
  const [scanning, setScanning] = useState(false);

  const hasTrimData = selParams?.startSec != null;
  const bagDuration = selParams?.duration ?? 0;
  const trimStart = selParams?.trimStart ?? 0;
  const trimEnd = selParams?.trimEnd ?? bagDuration;

  const trimInfo = useMemo(() => {
    if (!hasTrimData || !selParams?.metaFiles?.length) return null;
    const absStart = selParams.startSec + trimStart;
    const absEnd = selParams.startSec + trimEnd;
    const matched = selParams.metaFiles.filter(f => f.start_sec < absEnd && f.end_sec > absStart);
    const hasDataCat = PARAMS.some(p => selParams[p.key]);
    const zeroFile = selParams.metaFiles.find(f => f.path.endsWith("_0.parquet"));
    const zeroIncluded = hasDataCat && zeroFile && !matched.some(f => f.path === zeroFile.path);
    const totalMatched = matched.length + (zeroIncluded ? 1 : 0);
    const totalFiles = selParams.metaFiles.length;
    const cs = dataset.files?.cat_sizes;
    let trimBytes = null;
    if (cs && totalFiles > 0) {
      const msgBytes = (cs.lidars || 0) + (cs.cameras || 0) + (cs.radars || 0) + (cs.other || 0);
      trimBytes = Math.round(msgBytes * totalMatched / totalFiles) + (cs.topics || 0) + (cs.meta || 0);
    }
    return { matched: totalMatched, total: totalFiles, trimBytes, zeroForced: !!zeroIncluded };
  }, [hasTrimData, selParams, trimStart, trimEnd, dataset.files]);

  const allChecked = useMemo(() => {
    if (!selParams) return false;
    return PARAMS.every(p => selParams[p.key]) && selParams.topics !== false && selParams.meta !== false;
  }, [selParams]);

  const estData = useMemo(() => {
    if (!selParams) return null;
    const cs = dataset.files?.cat_sizes;
    if (!cs) {
      const ratios = { lidars: 0.35, cameras: 0.45, radars: 0.1, other: 0.15 };
      let pct = 0;
      if (selParams.topics !== false) pct += 0.03;
      if (selParams.meta !== false) pct += 0.02;
      for (const p of PARAMS) if (selParams[p.key] && dataset.files[p.key] > 0) pct += ratios[p.key];
      return { gb: (dataset.size_bytes / 1073741824 * pct).toFixed(2), approx: true };
    }
    let bytes = 0;
    if (selParams.topics !== false) bytes += (cs.topics || 0);
    if (selParams.meta !== false) bytes += (cs.meta || 0);
    for (const p of PARAMS) if (selParams[p.key]) bytes += (cs[p.key] || 0);
    return { gb: (bytes / 1073741824).toFixed(2), approx: false };
  }, [dataset, selParams]);

  const handleToggleAll = () => {
    const newVal = !allChecked;
    const patch = {};
    for (const p of PARAMS) patch[p.key] = newVal;
    patch.topics = newVal ? true : false;
    patch.meta = newVal ? true : false;
    onSetAll(dataset.id, patch);
  };

  return (
    <div style={{
      background: isSelected ? t.selectedBg : t.surface,
      border: `1px solid ${isSelected ? t.accent : t.border}`,
      borderRadius: 6, transition: "all 0.12s",
      boxShadow: theme === "light" && !isSelected ? "0 1px 3px rgba(0,0,0,0.06)" : "none",
    }}>
      {/* Header row */}
      <div onClick={onToggle} style={{ padding: "12px 18px", cursor: "pointer", display: "flex", alignItems: "center", gap: 14, userSelect: "none" }}>
        <div style={{
          width: 18, height: 18, border: `1px solid ${isSelected ? t.accent : t.border}`,
          borderRadius: 4, background: isSelected ? t.accent : "transparent",
          display: "flex", alignItems: "center", justifyContent: "center",
          flexShrink: 0, transition: "all 0.12s",
        }}>
          {isSelected && <span style={{ color: "#000", fontSize: 11, fontWeight: 700 }}>&#10003;</span>}
        </div>

        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 13, color: isSelected ? t.accent : t.text, marginBottom: 4, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {dataset.name}
          </div>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <span style={{ fontSize: 11, color: t.textSecondary }}>{dataset.date}</span>
            <span style={{ fontSize: 11, color: "#f59e0b" }}>{dataset.device_id}</span>
            {PARAMS.map(p => dataset.files[p.key] > 0 && (
              <span key={p.key} style={{ fontSize: 11, color: p.color }}>{p.label} {dataset.files[p.key]}</span>
            ))}
          </div>
        </div>

        <div style={{ textAlign: "right", flexShrink: 0, display: "flex", alignItems: "center", gap: 10 }}>
          {isSelected && (
            <button
              onClick={e => { e.stopPropagation(); if (!scanning) { setScanning(true); onScanBag(dataset.id).finally(() => setScanning(false)); } }}
              disabled={scanning}
              style={{
                background: "transparent", border: `1px solid ${scanning ? t.accent : t.border}`,
                color: scanning ? t.accent : t.textSecondary, borderRadius: 4,
                padding: "4px 10px", fontSize: 11, cursor: scanning ? "wait" : "pointer",
                fontFamily: "inherit", letterSpacing: 0.5, transition: "all 0.15s", whiteSpace: "nowrap",
              }}
              onMouseEnter={e => { if (!scanning) { e.currentTarget.style.borderColor = t.accent; e.currentTarget.style.color = t.accent; } }}
              onMouseLeave={e => { if (!scanning) { e.currentTarget.style.borderColor = t.border; e.currentTarget.style.color = t.textSecondary; } }}
              title="Request exact size and files from S3"
            >{scanning ? "\u27F3 Scan..." : "\u2139 Size"}</button>
          )}
          {(isSelected && estData !== null) && (
            <div>
              <div style={{ fontSize: 12, color: t.accent }}>{estData.approx ? "~" : ""}{estData.gb} GB</div>
            </div>
          )}
          {(!isSelected && dataset.size_bytes > 0) && (
            <div style={{ fontSize: 12, color: t.textSecondary }}>{(dataset.size_bytes / 1073741824).toFixed(2)} GB</div>
          )}
        </div>
      </div>

      {/* Expanded panel */}
      {isSelected && (
        <div onClick={e => e.stopPropagation()} style={{ borderTop: `1px solid ${t.border}`, padding: "12px 18px 14px 50px" }}>
          {/* Category checkboxes */}
          <div style={{ display: "flex", gap: 16, alignItems: "center", marginBottom: 12, flexWrap: "wrap" }}>
            <div onClick={handleToggleAll} style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer" }}>
              <Checkbox checked={allChecked} color={t.accent} size={14} />
              <span style={{ fontSize: 12, color: allChecked ? t.accent : t.textSecondary, fontWeight: 600 }}>All</span>
            </div>
            <span style={{ color: t.border }}>|</span>
            {PARAMS.map(p => (
              <div key={p.key} onClick={() => onParamToggle(dataset.id, p.key)} style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer" }}>
                <Checkbox checked={selParams[p.key]} color={p.color} size={14} />
                <span style={{ fontSize: 12, color: selParams[p.key] ? p.color : t.textSecondary, transition: "color 0.15s" }}>
                  {p.label}
                  {dataset.files[p.key] > 0 && <span style={{ color: t.textSecondary }}> &middot;{dataset.files[p.key]}</span>}
                </span>
              </div>
            ))}
            <span style={{ color: t.border }}>|</span>
            <div onClick={() => onMetaToggle(dataset.id, "topics")} style={{ display: "flex", alignItems: "center", gap: 5, cursor: "pointer" }}>
              <Checkbox checked={selParams.topics !== false} color="#94a3b8" size={13} />
              <span style={{ fontSize: 11, color: selParams.topics !== false ? "#94a3b8" : t.textSecondary }}>topics</span>
            </div>
            <div onClick={() => onMetaToggle(dataset.id, "meta")} style={{ display: "flex", alignItems: "center", gap: 5, cursor: "pointer" }}>
              <Checkbox checked={selParams.meta !== false} color="#94a3b8" size={13} />
              <span style={{ fontSize: 11, color: selParams.meta !== false ? "#94a3b8" : t.textSecondary }}>meta</span>
            </div>
          </div>

          {/* Trim section */}
          <div style={{ borderTop: `1px solid ${t.border}22`, paddingTop: 10 }}>
            <div style={{ display: "flex", gap: 16 }}>
              <div style={{ flex: 1, minWidth: 0, minHeight: 36 }}>
                {selParams?.metaLoading ? (
                  <div style={{ padding: "10px 0", fontSize: 12, color: t.textSecondary }}>{"\u27F3"} Loading metadata...</div>
                ) : hasTrimData ? (
                  <>
                    <TrimSlider
                      startSec={selParams.startSec} duration={bagDuration}
                      trimStart={trimStart} trimEnd={trimEnd}
                      onTrimChange={(s, e) => onTrimChange(dataset.id, s, e)} theme={theme}
                    />
                    {trimInfo && (
                      <div style={{ fontSize: 11, color: t.textSecondary, marginTop: 5 }}>
                        Files: <span style={{ color: t.accent }}>{trimInfo.matched}</span>/{trimInfo.total}
                        {trimInfo.zeroForced && <span style={{ color: "#f59e0b", marginLeft: 6 }}>+_0 (one-shot topics)</span>}
                        {trimInfo.trimBytes != null && (
                          <span style={{ color: t.accent, marginLeft: 8 }}>{"\u2248"} {(trimInfo.trimBytes / 1073741824).toFixed(2)} GB</span>
                        )}
                      </div>
                    )}
                  </>
                ) : (
                  <button
                    onClick={() => onRequestTrim(dataset.id)}
                    style={{
                      background: "transparent", border: `1px solid ${t.border}`, color: t.textSecondary,
                      borderRadius: 4, padding: "8px 16px", fontSize: 11, cursor: "pointer",
                      fontFamily: "inherit", letterSpacing: 1, transition: "all 0.15s",
                    }}
                    onMouseEnter={e => { e.currentTarget.style.borderColor = t.accent; e.currentTarget.style.color = t.accent; }}
                    onMouseLeave={e => { e.currentTarget.style.borderColor = t.border; e.currentTarget.style.color = t.textSecondary; }}
                  >
                    {"\u2702"} Trim by time
                  </button>
                )}
              </div>

            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// =============================================================================
// Pager
// =============================================================================

function Pager({ page, pages, total, onChange, theme }) {
  const t = themes[theme];
  if (pages <= 1) return <div style={{ fontSize: 12, color: t.textSecondary, textAlign: "center", padding: "14px 0" }}>{total} datasets</div>;
  const nums = [...Array(pages)].map((_, i) => i + 1).filter(p => p === 1 || p === pages || Math.abs(p - page) <= 2);
  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 7, padding: "16px 0" }}>
      <span style={{ fontSize: 12, color: t.textSecondary, marginRight: 8 }}>{total} datasets</span>
      {[...Array(pages)].map((_, i) => {
        const p = i + 1;
        const show = nums.includes(p);
        const prev = nums.includes(p - 1) || p === 1;
        if (!show) return !prev ? null : <span key={p} style={{ fontSize: 12, color: t.textSecondary }}>&hellip;</span>;
        return (
          <button key={p} onClick={() => onChange(p)} style={{
            background: p === page ? t.accent : "transparent",
            color: p === page ? "#000" : t.textSecondary,
            border: `1px solid ${p === page ? t.accent : t.border}`,
            borderRadius: 4, width: 30, height: 26, fontSize: 12,
            cursor: "pointer", fontFamily: "inherit", transition: "all 0.12s",
          }}>{p}</button>
        );
      })}
    </div>
  );
}

// =============================================================================
// QueueRow
// =============================================================================

function QueueRow({ item, onStop, onResume, onRemove, theme }) {
  const t = themes[theme];
  const sc = STATUS_CFG[item.status] || STATUS_CFG.pending;
  const pct = item.total > 0 ? Math.round((item.downloaded + item.skipped) / item.total * 100) : 0;

  return (
    <div style={{ background: t.surface, border: `1px solid ${item.status === "done" ? "#1a3a20" : item.status === "stopped" ? "#1a2a3a" : t.border}`, borderRadius: 6, padding: "14px 18px", boxShadow: theme === "light" ? "0 1px 2px rgba(0,0,0,0.05)" : "none" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
        <div style={{ width: 10, height: 10, borderRadius: "50%", flexShrink: 0, background: sc.color, boxShadow: sc.glow ? `0 0 10px ${sc.color}` : "none", animation: sc.glow ? "glow-pulse 1.5s infinite" : "none" }} />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 13, color: t.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginBottom: 4 }}>{item.bag_name}</div>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            {PARAMS.filter(p => item.params?.[p.key]).map(p => (
              <span key={p.key} style={{ fontSize: 12, color: p.color }}>{p.label}</span>
            ))}
            {item.params?.topics !== false && <span style={{ fontSize: 11, color: "#94a3b8" }}>topics</span>}
            {item.params?.meta !== false && <span style={{ fontSize: 11, color: "#94a3b8" }}>meta</span>}
            {item.params?.trimStart != null && item.params?.trimEnd != null && item.params?.startSec && (
              <span style={{ fontSize: 11, color: "#a855f7" }}>
                {(() => {
                  const fmtTs = (epochSec) => new Date(epochSec * 1000).toLocaleTimeString("ru-RU", { hour12: false });
                  const s = item.params.startSec;
                  return `${fmtTs(s + item.params.trimStart)}\u2013${fmtTs(s + item.params.trimEnd)}`;
                })()}
              </span>
            )}
            {item.status === "downloading" && item.current_file && (
              <span style={{ fontSize: 11, color: "#f59e0b" }}>{item.current_file}</span>
            )}
          </div>
        </div>
        <div style={{ textAlign: "right", minWidth: 110 }}>
          <div style={{ fontSize: 12, color: sc.color, marginBottom: 3 }}>{sc.label}</div>
          {item.total > 0 && <div style={{ fontSize: 11, color: t.textSecondary }}>{item.downloaded + item.skipped}/{item.total} ({pct}%)</div>}
          {item.skipped > 0 && <div style={{ fontSize: 11, color: t.textSecondary }}>{item.skipped} skipped</div>}
        </div>
        <div style={{ minWidth: 100, textAlign: "right", display: "flex", gap: 6, justifyContent: "flex-end" }}>
          {(item.status === "downloading" || item.status === "pending") && (
            <button onClick={() => onStop(item.id)} style={{ background: "none", border: `1px solid ${t.border}`, color: t.textSecondary, borderRadius: 4, padding: "6px 12px", fontSize: 11, cursor: "pointer", fontFamily: "inherit", textTransform: "uppercase", letterSpacing: 1 }}>Stop</button>
          )}
          {(item.status === "stopped" || item.status === "error") && (
            <button onClick={() => onResume(item.id)} style={{ background: "none", border: "1px solid #06b6d4", color: "#06b6d4", borderRadius: 4, padding: "6px 12px", fontSize: 11, cursor: "pointer", fontFamily: "inherit", textTransform: "uppercase", letterSpacing: 1 }}>Resume</button>
          )}
          {(item.status === "stopped" || item.status === "done" || item.status === "error") && (
            <button onClick={() => onRemove(item.id)} style={{ background: "none", border: `1px solid ${t.border}`, color: t.textSecondary, borderRadius: 4, padding: "6px 12px", fontSize: 11, cursor: "pointer", fontFamily: "inherit" }}>&times;</button>
          )}
        </div>
      </div>
      {item.status === "downloading" && item.total > 0 && (
        <div style={{ marginTop: 10, background: t.border, borderRadius: 3, height: 4, overflow: "hidden" }}>
          <div style={{ width: `${pct}%`, height: "100%", background: sc.color, transition: "width 0.3s", borderRadius: 3 }} />
        </div>
      )}
      {item.status === "done" && item.dest && (
        <div style={{ marginTop: 7, fontSize: 11, color: t.textSecondary }}>{item.dest}</div>
      )}
      {item.status === "error" && item.error_msg && (
        <div style={{ marginTop: 10, fontSize: 11, color: "#7f1d1d", borderLeft: "2px solid #ef4444", paddingLeft: 10 }}>{item.error_msg}</div>
      )}
    </div>
  );
}

// =============================================================================
// DebugPanel
// =============================================================================

function DebugPanel({ logs, onClear, theme, onToggle }) {
  const t = themes[theme];
  const [expanded, setExpanded] = useState({});
  const errCount = logs.filter(l => l.level === "error").length;

  return (
    <div style={{ position: "fixed", bottom: 0, left: 0, right: 0, zIndex: 140, fontFamily: "'IBM Plex Mono','Courier New',monospace" }}>
      <div onClick={onToggle} style={{
        background: t.surface, borderTop: `1px solid ${errCount > 0 ? "#ef4444" : t.border}`,
        padding: "6px 18px", display: "flex", alignItems: "center", gap: 14,
        cursor: "pointer", userSelect: "none", fontSize: 11, letterSpacing: 1,
      }}>
        <span style={{ color: t.accent, fontSize: 10 }}>{"\u25BC"}</span>
        <span style={{ color: t.textSecondary }}>DEBUG</span>
        {logs.length > 0 && (
          <span style={{ color: errCount > 0 ? "#ef4444" : t.accent }}>
            {errCount > 0 ? `\u2717 ${errCount} ERR` : `\u25CF ${logs.length}`}
          </span>
        )}
        <span style={{ flex: 1 }} />
        <button
          onClick={e => { e.stopPropagation(); onClear(); }}
          style={{ background: "none", border: `1px solid ${t.border}`, color: t.textSecondary, borderRadius: 3, padding: "2px 10px", fontSize: 10, cursor: "pointer", fontFamily: "inherit" }}
        >clear</button>
      </div>
      <div style={{ background: t.bg, borderTop: `1px solid ${t.border}`, maxHeight: DEBUG_PANEL_H, overflowY: "auto" }}>
        {logs.length === 0
          ? <div style={{ padding: "14px 18px", color: t.textSecondary, fontSize: 11 }}>// empty</div>
          : logs.map(entry => (
            <div key={entry.id}
              onClick={() => entry.data && setExpanded(e => ({ ...e, [entry.id]: !e[entry.id] }))}
              style={{ padding: "3px 18px", borderBottom: `1px solid ${t.border}22`, cursor: entry.data ? "pointer" : "default", lineHeight: 1.7 }}
            >
              <span style={{ color: t.textSecondary, fontSize: 10, marginRight: 10 }}>{entry.ts}</span>
              <span style={{ color: LOG_COLORS[entry.level] || "#06b6d4", fontSize: 10, marginRight: 10 }}>[{entry.level.toUpperCase()}]</span>
              <span style={{ color: t.text, fontSize: 11 }}>{entry.msg}</span>
              {entry.data && <span style={{ color: t.textSecondary, fontSize: 10, marginLeft: 8 }}>{expanded[entry.id] ? "\u25B4" : "\u25BE"}</span>}
              {entry.data && expanded[entry.id] && (
                <pre style={{ color: t.textSecondary, fontSize: 10, margin: "3px 0 5px 26px", background: t.surface, padding: "8px 10px", borderRadius: 4, overflow: "auto", maxHeight: 160, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
                  {JSON.stringify(entry.data, null, 2)}
                </pre>
              )}
            </div>
          ))
        }
      </div>
    </div>
  );
}

// =============================================================================
// HelpModal
// =============================================================================

function HelpModal({ theme, onClose }) {
  const t = themes[theme];
  return (
    <div onClick={onClose} style={{ position: "fixed", inset: 0, zIndex: 200, background: "rgba(0,0,0,0.78)", display: "flex", justifyContent: "center", alignItems: "flex-start", padding: "40px 16px", overflowY: "auto" }}>
      <div onClick={e => e.stopPropagation()} style={{ background: t.surface, border: `1px solid ${t.border}`, borderRadius: 10, maxWidth: 700, width: "100%", padding: "32px 36px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 28 }}>
          <span style={{ fontSize: 14, fontWeight: 700, color: t.text, letterSpacing: 3, textTransform: "uppercase" }}>Help</span>
          <button onClick={onClose} style={{ background: "none", border: `1px solid ${t.border}`, color: t.textSecondary, borderRadius: 4, padding: "5px 14px", fontSize: 11, cursor: "pointer", fontFamily: "inherit" }}>&times; Close</button>
        </div>

        <div style={{ marginBottom: 32 }}>
          <div style={{ fontSize: 11, letterSpacing: 2, textTransform: "uppercase", color: t.textSecondary, marginBottom: 14 }}>How to use</div>
          {[
            "Find the right dataset using search or by scrolling",
            "Click on a dataset row to select it",
            "Press Size button to get exact file data",
            "Select data categories: Lidars, Cameras, Radars, Other",
            "Optionally use time-based trimming",
            "Press \u25B6 Download",
            "Go to the Queue tab to track progress",
          ].map((step, i) => (
            <div key={i} style={{ display: "flex", gap: 12, marginBottom: 7, alignItems: "flex-start" }}>
              <span style={{ fontSize: 11, color: t.accent, fontWeight: 700, minWidth: 16, paddingTop: 2 }}>{i + 1}.</span>
              <span style={{ fontSize: 12, color: t.text, lineHeight: 1.5 }}>{step}</span>
            </div>
          ))}
        </div>

        <div style={{ fontSize: 11, color: t.textSecondary, lineHeight: 1.8 }}>
          <div style={{ marginBottom: 8 }}><span style={{ color: "#f59e0b" }}>&bull;</span> Downloading — files are being transferred from MinIO</div>
          <div style={{ marginBottom: 8 }}><span style={{ color: "#6b7280" }}>&bull;</span> Pending — waiting to download</div>
          <div style={{ marginBottom: 8 }}><span style={{ color: "#10b981" }}>&bull;</span> Done — all files downloaded</div>
          <div style={{ marginBottom: 8 }}><span style={{ color: "#06b6d4" }}>&bull;</span> Stopped — you can resume</div>
        </div>
      </div>
    </div>
  );
}

// =============================================================================
// App (main)
// =============================================================================

export default function App() {
  const [tab, setTab]       = useState("datasets");
  const [search, setSearch] = useState("");
  const [page, setPage]     = useState(1);
  const [sel, setSel]       = useState({});
  const [theme, setTheme]   = useState(() => {
    try { return JSON.parse(localStorage.getItem("bl_settings") || "{}").theme || "dark"; }
    catch { return "dark"; }
  });
  const [refreshing, setRefreshing] = useState(false);
  const [showHelp, setShowHelp]     = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [scanCache, setScanCache]   = useState({});

  // Settings (persisted in localStorage)
  const [settings, setSettings] = useState(() => {
    try { return { showDebug: false, ...JSON.parse(localStorage.getItem("bl_settings") || "{}") }; }
    catch { return { showDebug: false }; }
  });
  const updateSettings = useCallback((patch) => {
    setSettings(s => {
      const n = { ...s, ...patch };
      localStorage.setItem("bl_settings", JSON.stringify(n));
      return n;
    });
  }, []);
  const [settingsOpen, setSettingsOpen] = useState(false);

  const dSearch = useDebounce(search, 400);
  useEffect(() => setPage(1), [dSearch]);

  const { datasets: rawBags, total, pages, loading, error } = useBags(dSearch, page, refreshKey);

  // Merge scan cache into bags (avoids full refresh that resets scroll)
  const datasets = useMemo(() => rawBags.map(b => {
    const sc = scanCache[b.id];
    if (!sc) return b;
    return { ...b, size_bytes: sc.size_bytes, files: { ...b.files, ...sc.files, cat_sizes: sc.cat_sizes } };
  }), [rawBags, scanCache]);

  const { online: backendOnline, recheck: recheckHealth } = useHealth();
  const onQueueFetchError = useCallback(() => { recheckHealth(); }, [recheckHealth]);
  const { queue, addToQueue, stopItem, resumeItem, removeItem, clearDone } = useQueue(onQueueFetchError);
  const { logs, addLog, clearLog } = useLog();

  const selCount = useMemo(() => Object.keys(sel).length, [sel]);
  const activeQ  = queue.filter(q => q.status !== "done" && q.status !== "error");
  const pendingN = queue.filter(q => q.status === "pending").length;
  const dlN      = queue.filter(q => q.status === "downloading").length;
  const doneN    = queue.filter(q => q.status === "done").length;
  const stoppedN = queue.filter(q => q.status === "stopped").length;

  const [downloadPath, setDownloadPath] = useState("");
  useEffect(() => {
    fetch(`${API_BASE}/api/download-path`).then(r => r.json()).then(d => setDownloadPath(d.path)).catch(() => {});
  }, []);

  // -- Callbacks --

  const toggleDataset = useCallback((dataset) => {
    setSel(s => {
      if (s[dataset.id]) {
        const { [dataset.id]: _, ...rest } = s;
        return rest;
      }
      return { ...s, [dataset.id]: { lidars: false, cameras: false, radars: false, other: false, topics: false, meta: true } };
    });
  }, []);

  const requestTrim = useCallback((bagId) => {
    setSel(s => {
      const cur = s[bagId];
      if (!cur) return s;
      return { ...s, [bagId]: { ...cur, metaLoading: true } };
    });
    fetch(`${API_BASE}/api/datasets/${encodeURIComponent(bagId)}/metadata`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(meta => {
        const dur = Math.round(meta.duration_sec);
        setSel(s => {
          const cur = s[bagId];
          if (!cur) return s;
          return { ...s, [bagId]: { ...cur, duration: dur, startSec: meta.start_sec, trimStart: 0, trimEnd: dur, metaFiles: meta.files, metaFileCount: meta.files?.length || 0, metaLoading: false } };
        });
        addLog("ok", `Metadata ${bagId}: ${dur}s, ${meta.files?.length || 0} files`);
      })
      .catch((err) => {
        setSel(s => {
          const cur = s[bagId];
          if (!cur) return s;
          return { ...s, [bagId]: { ...cur, metaLoading: false } };
        });
        addLog("error", `Metadata ${bagId}: ${err}`);
      });
  }, [addLog]);

  const toggleParam = useCallback((bagId, paramKey) => {
    setSel(s => {
      const cur = { ...s[bagId] };
      const newVal = !cur[paramKey];
      cur[paramKey] = newVal;
      if (newVal) {
        // Enabled data category → automatically enable topics
        if (cur.topics === false) cur.topics = true;
      } else {
        // Disabled category → if no categories left, disable topics
        if (!PARAMS.some(p => cur[p.key])) cur.topics = false;
      }
      return { ...s, [bagId]: cur };
    });
  }, []);

  const setAllParams = useCallback((bagId, patch) => {
    setSel(s => ({ ...s, [bagId]: { ...s[bagId], ...patch } }));
  }, []);

  const toggleMeta = useCallback((bagId, key) => {
    setSel(s => {
      const cur = s[bagId];
      if (!cur) return s;
      return { ...s, [bagId]: { ...cur, [key]: cur[key] !== false ? false : true } };
    });
  }, []);

  const handleTrimChange = useCallback((bagId, start, end) => {
    setSel(s => {
      const cur = s[bagId];
      if (!cur) return s;
      return { ...s, [bagId]: { ...cur, trimStart: start, trimEnd: end } };
    });
  }, []);

  const handleScanBag = useCallback(async (bagId) => {
    try {
      const res = await fetch(`${API_BASE}/api/datasets/${encodeURIComponent(bagId)}/scan`, { method: "POST" });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      addLog("ok", `Scan ${bagId}: ${(data.size_bytes / 1073741824).toFixed(2)} GB`);
      setScanCache(c => ({ ...c, [bagId]: data }));
    } catch (e) {
      addLog("error", `Scan ${bagId}: ${e.message}`);
    }
  }, [addLog]);

  const handleAddToQueue = useCallback(async () => {
    const items = Object.entries(sel).map(([bag_name, params]) => ({ bag_name, params }));
    addLog("info", `\u25B6 Download: ${items.length} dataset(s)`);
    try {
      await addToQueue(items);
      addLog("ok", "Added to queue");
    } catch (e) {
      addLog("error", `Error: ${e.message}`);
    }
    setSel({});
    setTab("queue");
  }, [sel, addToQueue, addLog]);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    addLog("info", "Syncing...");
    try {
      const res = await fetch(`${API_BASE}/api/datasets/refresh`, { method: "POST" });
      const body = await res.json();
      if (res.ok) {
        addLog("ok", `Index updated: ${body.message || "OK"}`);
        setRefreshKey(k => k + 1);
      } else {
        addLog("error", `HTTP Error ${res.status}: ${body.detail || res.statusText}`, body);
      }
    } catch (e) {
      addLog("error", `Sync error: ${e.message}`);
    } finally {
      setRefreshing(false);
    }
  }, [addLog]);

  const t = themes[theme];
  const bottomOffset = settings.showDebug ? DEBUG_BAR_H + DEBUG_PANEL_H : 0;

  // -- Render --

  return (
    <div onClick={() => settingsOpen && setSettingsOpen(false)} style={{ minHeight: "100vh", background: t.bg, fontFamily: "'IBM Plex Mono','Courier New',monospace", color: t.text, transition: "background 0.2s, color 0.2s" }}>
      {theme === "dark" && <div style={{ position: "fixed", inset: 0, pointerEvents: "none", zIndex: 100, background: "repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,0.02) 2px,rgba(0,0,0,0.02) 4px)" }} />}

      {/* Header */}
      <div style={{ background: t.surface, borderBottom: `1px solid ${t.border}`, padding: "0 32px", display: "flex", alignItems: "center", gap: 32, height: 60, position: "sticky", top: 0, zIndex: 50, boxShadow: theme === "light" ? "0 1px 3px rgba(0,0,0,0.08)" : "none" }}>
        <div onClick={() => setTab("datasets")} style={{ display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }}>
          <span style={{ color: t.accent, fontSize: 20 }}>{"\u25A3"}</span>
          <span style={{ fontSize: 15, fontWeight: 700, letterSpacing: 3, color: t.text, textTransform: "uppercase" }}>DATA<span style={{ color: t.accent }}>MANAGER</span></span>
        </div>
        <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
          {["datasets", "queue"].map(tabKey => (
            <button key={tabKey} onClick={() => setTab(tabKey)} style={{ background: "none", border: "none", cursor: "pointer", color: tab === tabKey ? t.accent : t.textSecondary, fontSize: 12, letterSpacing: 2, textTransform: "uppercase", paddingBottom: 5, borderBottom: tab === tabKey ? `2px solid ${t.accent}` : "2px solid transparent", transition: "all 0.15s" }}>
              {tabKey === "datasets" ? "Datasets" : `Queue${activeQ.length ? ` [${activeQ.length}]` : ""}`}
            </button>
          ))}
        </div>
        <div style={{ flex: 1 }} />
        {backendOnline === false
          ? <span style={{ fontSize: 11, color: "#ef4444", letterSpacing: 1 }}>{"\u26A0"} OFFLINE</span>
          : backendOnline === null
            ? <span style={{ fontSize: 11, color: t.textSecondary, letterSpacing: 1 }}>{"\u25CB"} ...</span>
            : <span style={{ fontSize: 11, color: t.accent, letterSpacing: 1 }}>{"\u25CF"} ONLINE</span>
        }
        <div style={{ position: "relative" }}>
          <button onClick={() => setSettingsOpen(o => !o)}
            style={{ background: settingsOpen ? t.accent + "22" : "none", border: `1px solid ${settingsOpen ? t.accent : t.border}`, color: settingsOpen ? t.accent : t.textSecondary, borderRadius: 4, width: 30, height: 30, fontSize: 15, cursor: "pointer", fontFamily: "inherit", display: "flex", alignItems: "center", justifyContent: "center", transition: "all 0.15s" }}
            title="Settings"
          >{"\u2699"}</button>
          {settingsOpen && (
            <div style={{ position: "absolute", right: 0, top: 38, background: t.surface, border: `1px solid ${t.border}`, borderRadius: 8, padding: "16px 20px", minWidth: 280, zIndex: 200, boxShadow: "0 8px 32px rgba(0,0,0,0.5)" }} onClick={e => e.stopPropagation()}>
              <div style={{ fontSize: 11, letterSpacing: 2, textTransform: "uppercase", color: t.textSecondary, marginBottom: 14 }}>Settings</div>

              <label style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12, cursor: "pointer", fontSize: 12, color: t.text }}>
                <input type="checkbox" checked={theme === "dark"} onChange={() => { const nv = theme === "dark" ? "light" : "dark"; setTheme(nv); updateSettings({ theme: nv }); }} style={{ accentColor: t.accent }} />
                {"\u263E"} Dark theme
              </label>

              <label style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12, cursor: "pointer", fontSize: 12, color: t.text }}>
                <input type="checkbox" checked={settings.showDebug} onChange={() => updateSettings({ showDebug: !settings.showDebug })} style={{ accentColor: t.accent }} />
                Debug console
              </label>

              <div style={{ borderTop: `1px solid ${t.border}`, paddingTop: 12, marginTop: 8 }}>
                <div style={{ fontSize: 11, color: t.textSecondary, marginBottom: 8 }}>Download folder</div>
                <form onSubmit={e => {
                  e.preventDefault();
                  const val = e.target.elements.dlpath.value.trim();
                  if (!val) return;
                  const btn = e.target.querySelector("button[type=submit]");
                  const input = e.target.elements.dlpath;
                  btn.textContent = "...";
                  btn.disabled = true;
                  input.style.borderColor = t.border;
                  fetch(`${API_BASE}/api/download-path`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ path: val }) })
                    .then(r => r.ok ? r.json() : r.json().then(j => Promise.reject(j.detail || r.statusText)))
                    .then(d => {
                      setDownloadPath(d.path);
                      addLog("ok", `Folder: ${d.path}`);
                      btn.textContent = "\u2713";
                      input.style.borderColor = t.accent;
                      input.value = d.path;
                      setTimeout(() => { input.style.borderColor = t.border; }, 2000);
                    })
                    .catch(err => {
                      addLog("error", `Folder: ${err}`);
                      btn.textContent = "\u2717";
                      input.style.borderColor = "#ef4444";
                      setTimeout(() => { btn.textContent = "\u2713"; input.style.borderColor = t.border; }, 3000);
                    })
                    .finally(() => { btn.disabled = false; });
                }} style={{ display: "flex", gap: 6 }}>
                  <input name="dlpath" defaultValue={downloadPath} style={{ flex: 1, background: t.bg, border: `1px solid ${t.border}`, borderRadius: 4, padding: "6px 10px", color: t.text, fontSize: 11, fontFamily: "inherit", transition: "border-color 0.2s" }} />
                  <button type="submit" style={{ background: "none", border: `1px solid ${t.border}`, color: t.textSecondary, borderRadius: 4, padding: "6px 10px", fontSize: 11, cursor: "pointer", fontFamily: "inherit", minWidth: 30, transition: "all 0.2s" }}>{"\u2713"}</button>
                </form>
                <div style={{ fontSize: 10, color: t.textSecondary, marginTop: 5 }}>~/path or subfolder. Example: ~/datasets, ~/work_folder</div>
              </div>

              <div style={{ borderTop: `1px solid ${t.border}`, paddingTop: 12, marginTop: 12, display: "flex", gap: 8 }}>
                <button onClick={() => setShowHelp(true)} style={{ background: "none", border: `1px solid ${t.border}`, color: t.textSecondary, borderRadius: 4, padding: "6px 14px", fontSize: 11, cursor: "pointer", fontFamily: "inherit" }}>? Help</button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Content */}
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "32px 24px", paddingBottom: bottomOffset + 60 }}>

        {/* Bags tab */}
        {tab === "datasets" && (<>
          <div style={{ display: "flex", gap: 14, marginBottom: 16, flexWrap: "wrap" }}>
            <div style={{ flex: 1, minWidth: 240, position: "relative" }}>
              <span style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: t.accent, fontSize: 14 }}>{"\u2315"}</span>
              <input
                value={search} onChange={e => setSearch(e.target.value)} placeholder="Search by name, device ID..."
                style={{ width: "100%", boxSizing: "border-box", background: t.surface, border: `1px solid ${t.border}`, borderRadius: 6, padding: "10px 14px 10px 36px", color: t.text, fontSize: 13, outline: "none" }}
                onFocus={e => e.target.style.borderColor = t.accent}
                onBlur={e => e.target.style.borderColor = t.border}
              />
              {(loading || refreshing) && <span style={{ position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)", fontSize: 11, color: t.accent, letterSpacing: 1, animation: "pulse 1s infinite" }}>{"\u27F3"} {refreshing ? "SYNC..." : "..."}</span>}
            </div>
            <button onClick={handleRefresh} disabled={refreshing || loading}
              style={{ background: refreshing ? t.border : t.surface, border: `1px solid ${t.border}`, color: refreshing ? t.textSecondary : t.accent, borderRadius: 6, padding: "10px 16px", fontSize: 12, cursor: refreshing ? "not-allowed" : "pointer", fontFamily: "inherit", textTransform: "uppercase", letterSpacing: 1, transition: "all 0.2s", opacity: refreshing ? 0.6 : 1 }}
            >{"\u27F3"} Refresh</button>
          </div>

          {error && (
            <div style={{ background: "#1a0808", border: "1px solid #7f1d1d", borderRadius: 6, padding: "14px 18px", fontSize: 13, color: "#ef4444", marginBottom: 14 }}>
              No connection to backend: {error}
            </div>
          )}

          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {loading
              ? [...Array(8)].map((_, i) => <Skeleton key={i} theme={theme} />)
              : datasets.length === 0
                ? <div style={{ textAlign: "center", color: t.textSecondary, fontSize: 14, padding: 72 }}>// No datasets found</div>
                : datasets.map(dataset => (
                  <DatasetRow key={dataset.id} dataset={dataset} selParams={sel[dataset.id]}
                    onToggle={() => toggleDataset(dataset)} onParamToggle={toggleParam} onMetaToggle={toggleMeta} onSetAll={setAllParams}
                    onTrimChange={handleTrimChange} onRequestTrim={requestTrim} onScanBag={handleScanBag}
                    theme={theme}
                  />
                ))
            }
          </div>

          <Pager page={page} pages={pages} total={total} onChange={setPage} theme={theme} />
          {!loading && datasets.length > 0 && (
            <div style={{ fontSize: 12, color: t.textSecondary, textAlign: "center", marginBottom: 44 }}>
              Showing {datasets.length} of {total} &middot; Page {page}/{pages}
            </div>
          )}

          {selCount > 0 && (
            <div style={{ position: "fixed", bottom: bottomOffset + 14, left: "50%", transform: "translateX(-50%)", background: t.surface, border: `1px solid ${t.border}`, borderRadius: 8, padding: "14px 24px", display: "flex", alignItems: "center", gap: 20, boxShadow: "0 8px 32px rgba(0,0,0,0.7)", zIndex: 60, animation: "slideUp 0.2s ease", transition: "bottom 0.2s ease" }}>
              <span style={{ fontSize: 13, color: t.text }}>Selected: <span style={{ color: t.accent, fontWeight: 700 }}>{selCount}</span></span>
              <button onClick={() => setSel({})} style={{ background: "none", border: `1px solid ${t.border}`, color: t.textSecondary, borderRadius: 4, padding: "7px 14px", fontSize: 11, cursor: "pointer", fontFamily: "inherit" }}>Reset</button>
              <button onClick={handleAddToQueue} style={{ background: t.accent, color: "#000", border: "none", borderRadius: 5, padding: "8px 20px", fontSize: 12, letterSpacing: 1.5, textTransform: "uppercase", cursor: "pointer", fontFamily: "inherit", fontWeight: 700, transition: "all 0.15s" }}>{"\u25B6"} Download</button>
              {downloadPath && <span style={{ fontSize: 11, color: t.textSecondary }}>{"\u2192"} {downloadPath}</span>}
            </div>
          )}
        </>)}
      </div>
    </div>
  );
}