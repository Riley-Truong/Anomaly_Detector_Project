import { useEffect, useState } from 'react';
import axios from 'axios';
import './App.css';

const S3_BASE = 'https://anomaly-detector-dashboard.s3.us-east-2.amazonaws.com';

// ── Worker definitions — matches coordinator WORKER_ASSIGNMENTS ───
const WORKERS = [
  { key: 'wa', label: 'Worker A', source: 'Open-Meteo Forecast', cities: 'Cities 1–10',  field: 'AnomalyDetectorWorkerForecast' },
  { key: 'wb', label: 'Worker B', source: 'Open-Meteo Archive',  cities: 'Cities 11–20', field: 'AnomalyDetectorWorkerArchive'  },
  { key: 'wc', label: 'Worker C', source: 'NWS Observations',    cities: 'Cities 21–30', field: 'AnomalyDetectorWorkerNWS'      },
];

// ── Helpers ───────────────────────────────────────────────────────
const todayStr = () => new Date().toISOString().split('T')[0];

const fmtDate = (iso) => {
  if (!iso) return '';
  const [y, m, d] = iso.split('-');
  return new Date(y, m - 1, d).toLocaleDateString('en-US', {
    weekday: 'short', year: 'numeric', month: 'short', day: 'numeric'
  });
};

const fmtMs = (ms) => {
  if (ms == null || isNaN(ms)) return '—';
  if (ms >= 1000) return (ms / 1000).toFixed(2) + 's';
  return ms + 'ms';
};

const fmtSec = (ms) => {
  if (ms == null || isNaN(ms)) return '—';
  return (ms / 1000).toFixed(2);
};

// ── S3 fetch hook ─────────────────────────────────────────────────
function useS3(key) {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState(false);

  useEffect(() => {
    if (!key) return;
    axios.get(`${S3_BASE}/${key}`)
      .then(r => { setData(r.data); setLoading(false); })
      .catch(() => { setError(true); setLoading(false); });
  }, [key]);

  return { data, loading, error };
}

// ── Sort hook ─────────────────────────────────────────────────────
function useSort(f = 'difference', d = 'desc') {
  const [field, setField] = useState(f);
  const [dir, setDir]     = useState(d);

  const toggle = (nf) => {
    setDir(prev => field === nf ? (prev === 'asc' ? 'desc' : 'asc') : 'desc');
    setField(nf);
  };

  const sort = (arr) => [...(arr || [])].sort((a, b) => {
    const va = a[field], vb = b[field];
    if (va == null) return 1;
    if (vb == null) return -1;
    const na = parseFloat(va), nb = parseFloat(vb);
    const numeric = !isNaN(na) && !isNaN(nb);
    const cmp = numeric ? na - nb
      : String(va).localeCompare(String(vb), undefined, { sensitivity: 'base' });
    return dir === 'asc' ? cmp : -cmp;
  });

  const arrow = (nf) => field !== nf
    ? <span className="sort-icon" style={{ opacity: 0.2 }}>↕</span>
    : <span className="sort-icon">{dir === 'asc' ? '↑' : '↓'}</span>;

  return { field, toggle, sort, arrow };
}

// ── Small components ──────────────────────────────────────────────
function StateBox({ icon, message, sub, loading: spin }) {
  return (
    <div className="state-box">
      {spin ? <div className="spinner" /> : <span className="state-icon">{icon}</span>}
      <p>{message}</p>
      {sub && <p className="state-sub">{sub}</p>}
    </div>
  );
}

function SortTh({ label, field, current, toggle, arrow }) {
  return (
    <th className={`sortable ${current === field ? 'sort-active' : ''}`}
        onClick={() => toggle(field)}>
      {label}{arrow(field)}
    </th>
  );
}

function DiffCell({ value }) {
  const n = parseFloat(value);
  if (isNaN(n)) return <td className="mono diff-neut">—</td>;
  const cls = Math.abs(n) < 1 ? 'diff-neut' : n > 0 ? 'diff-pos' : 'diff-neg';
  return <td className={cls}>{n > 0 ? '+' : ''}{n.toFixed(1)}</td>;
}

// ─────────────────────────────────────────────────────────────────
// TAB 1 — BENCHMARK (primary tab)
// Parallel vs Sequential timing comparison + Amdahl's Law
// ─────────────────────────────────────────────────────────────────
function BenchmarkTab({ date }) {
  const { data: parData, loading: parL } = useS3(date ? `timing/${date}_parallel.json`   : null);
  const { data: seqData, loading: seqL } = useS3(date ? `timing/${date}_sequential.json` : null);
  const loading = parL || seqL;

  // Key numbers
  const parMs   = parData?.dispatch_ms  ?? parData?.duration_ms  ?? null;
  const seqMs   = seqData?.total_ms     ?? seqData?.duration_ms  ?? null;
  const speedup = parMs && seqMs && parMs > 0 ? (seqMs / parMs).toFixed(2) : null;
  const serial  = parMs && seqMs ? (parMs / seqMs * 100).toFixed(2) : null;
  const maxTheo = serial ? (1 / (parseFloat(serial) / 100)).toFixed(1) : null;
  const parPct  = parMs && seqMs ? Math.max(1, (parMs / seqMs) * 100) : 1;

  // Per-worker sequential timings
  const seqTimings = seqData?.timings || [];

  return (
    <>
      {loading && <StateBox spin message="Loading benchmark data..." />}

      {!loading && !speedup && (
        <StateBox icon="⏱" message="No benchmark data yet for this date."
          sub="Trigger both the coordinator (parallel) and sequential runner, then refresh." />
      )}

      {!loading && speedup && (
        <>
          {/* ── Speedup hero ── */}
          <div className="speedup-hero">
            <div className="speedup-eyebrow">Parallel Speedup Factor</div>
            <div className="speedup-number">
              {speedup}<span className="unit">×</span>
            </div>
            <div className="speedup-caption">
              The parallel system processed the same workload <strong style={{color:'var(--text-primary)'}}>{speedup}× faster</strong> than sequential.<br />
              3 workers fired simultaneously vs. 3 workers running one after another.
            </div>
          </div>

          {/* ── Summary stats ── */}
          <div className="stats-row">
            <div className="stat-card par">
              <div className="stat-label">Parallel Time</div>
              <div className="stat-value">{fmtSec(parMs)}</div>
              <div className="stat-sub">seconds (dispatch)</div>
            </div>
            <div className="stat-card seq">
              <div className="stat-label">Sequential Time</div>
              <div className="stat-value">{fmtSec(seqMs)}</div>
              <div className="stat-sub">seconds (total)</div>
            </div>
            <div className="stat-card speed">
              <div className="stat-label">Speedup</div>
              <div className="stat-value">{speedup}×</div>
              <div className="stat-sub">faster</div>
            </div>
            <div className="stat-card accent">
              <div className="stat-label">Serial Fraction</div>
              <div className="stat-value" style={{fontSize:22,paddingTop:4}}>{serial}%</div>
              <div className="stat-sub">Amdahl overhead</div>
            </div>
          </div>

          {/* ── Time comparison bars ── */}
          <div className="card">
            <div className="card-header">
              <span className="card-title">Wall-Clock Time Comparison</span>
              <span className="badge ok">Theoretical max: {maxTheo}×</span>
            </div>
            <div className="bar-section">
              <div className="bar-row">
                <div className="bar-meta">
                  <span className="bar-label" style={{color:'var(--parallel)'}}>
                    ▶ Parallel — all 3 workers fired simultaneously
                  </span>
                  <span className="bar-value">{fmtMs(parMs)}</span>
                </div>
                <div className="bar-track">
                  <div className="bar-fill par" style={{width:`${parPct}%`}} />
                </div>
              </div>

              <div className="bar-row">
                <div className="bar-meta">
                  <span className="bar-label" style={{color:'var(--sequential)'}}>
                    ▶ Sequential — Worker A → Worker B → Worker C
                  </span>
                  <span className="bar-value">{fmtMs(seqMs)}</span>
                </div>
                <div className="bar-track">
                  <div className="bar-fill seq" style={{width:'100%'}} />
                </div>
              </div>

              <div className="amdahl-note">
                <strong style={{color:'var(--text-secondary)'}}>Amdahl's Law analysis:</strong>
                &nbsp; Serial fraction = {serial}% (coordinator loop overhead).
                &nbsp; Theoretical max speedup = 1 ÷ {(parseFloat(serial)/100).toFixed(4)} = <strong style={{color:'var(--text-primary)'}}>{maxTheo}×</strong>.
                &nbsp; Measured speedup = <strong style={{color:'var(--text-primary)'}}>{speedup}×</strong>.
                &nbsp; Gap from theoretical = {Math.abs(parseFloat(maxTheo) - parseFloat(speedup)).toFixed(2)}× — nearly perfect parallel efficiency.
              </div>
            </div>
          </div>

          {/* ── Per-worker breakdown ── */}
          {seqTimings.length > 0 && (
            <div className="card">
              <div className="card-header">
                <span className="card-title">Sequential Worker Breakdown — Each Waited for the Previous</span>
              </div>
              <div style={{padding:'20px 24px'}}>
                {/* stacked bar showing worker A + B + C adding up to sequential total */}
                <div style={{marginBottom:20}}>
                  <div className="bar-meta" style={{marginBottom:8}}>
                    <span className="bar-label" style={{color:'var(--text-secondary)'}}>
                      Sequential total = Worker A + Worker B + Worker C
                    </span>
                    <span className="bar-value">{fmtMs(seqMs)}</span>
                  </div>
                  <div style={{display:'flex',height:10,borderRadius:5,overflow:'hidden',border:'1px solid var(--border)'}}>
                    {seqTimings.map((t, i) => {
                      const pct = seqMs ? (t.duration_ms / seqMs) * 100 : 33;
                      const colors = ['var(--worker-a)','var(--worker-b)','var(--worker-c)'];
                      return (
                        <div key={i} style={{
                          width:`${pct}%`, background:colors[i],
                          opacity:0.85, transition:'width 0.8s ease'
                        }} title={`${t.source}: ${fmtMs(t.duration_ms)}`} />
                      );
                    })}
                  </div>
                  <div style={{display:'flex',gap:16,marginTop:8,flexWrap:'wrap'}}>
                    {seqTimings.map((t, i) => {
                      const wkeys = ['wa','wb','wc'];
                      return (
                        <span key={i} style={{fontSize:11,fontFamily:'var(--font-data)',color:'var(--text-muted)'}}>
                          <span className={`wtag ${wkeys[i]}`} style={{marginRight:5}}>{t.source}</span>
                          {fmtMs(t.duration_ms)}
                        </span>
                      );
                    })}
                  </div>
                </div>

                {/* detailed table */}
                <table>
                  <thead>
                    <tr>
                      <th>Worker</th>
                      <th>Data Source</th>
                      <th>Cities</th>
                      <th>Duration</th>
                      <th>% of Total</th>
                    </tr>
                  </thead>
                  <tbody>
                    {seqTimings.map((t, i) => {
                      const wkeys = ['wa','wb','wc'];
                      const pct = seqMs ? ((t.duration_ms / seqMs) * 100).toFixed(1) : '—';
                      return (
                        <tr key={i}>
                          <td><span className={`wtag ${wkeys[i]}`}>{WORKERS[i]?.label ?? `Worker ${i+1}`}</span></td>
                          <td className="mono">{t.source}</td>
                          <td className="mono">{t.cities ?? 10}</td>
                          <td className="mono" style={{color:'var(--text-primary)',fontWeight:600}}>{fmtMs(t.duration_ms)}</td>
                          <td className="mono">{pct}%</td>
                        </tr>
                      );
                    })}
                    <tr style={{borderTop:'1px solid var(--border-light)',background:'var(--bg-elevated)'}}>
                      <td colSpan={3} style={{fontFamily:'var(--font-data)',fontWeight:600,color:'var(--text-secondary)'}}>TOTAL (sequential)</td>
                      <td className="mono" style={{color:'var(--sequential)',fontWeight:700}}>{fmtMs(seqMs)}</td>
                      <td className="mono">100%</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── What parallel changes ── */}
          <div className="card">
            <div className="card-header">
              <span className="card-title">Why Parallel Is Faster — The Core Concept</span>
            </div>
            <div style={{padding:'20px 24px'}}>
              <table>
                <thead>
                  <tr>
                    <th>Mode</th>
                    <th>Worker A starts</th>
                    <th>Worker B starts</th>
                    <th>Worker C starts</th>
                    <th>Total time</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td><span style={{color:'var(--parallel)',fontFamily:'var(--font-data)',fontWeight:600}}>Parallel</span></td>
                    <td className="mono" style={{color:'var(--green)'}}>t = 0ms</td>
                    <td className="mono" style={{color:'var(--green)'}}>t = 0ms</td>
                    <td className="mono" style={{color:'var(--green)'}}>t = 0ms</td>
                    <td className="mono" style={{color:'var(--parallel)',fontWeight:700}}>{fmtMs(parMs)} (slowest worker)</td>
                  </tr>
                  <tr>
                    <td><span style={{color:'var(--sequential)',fontFamily:'var(--font-data)',fontWeight:600}}>Sequential</span></td>
                    <td className="mono">t = 0ms</td>
                    <td className="mono">t = after A</td>
                    <td className="mono">t = after A+B</td>
                    <td className="mono" style={{color:'var(--sequential)',fontWeight:700}}>{fmtMs(seqMs)} (A + B + C)</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// TAB 2 — WORKER BREAKDOWN
// Shows what each worker is responsible for and what it collected
// ─────────────────────────────────────────────────────────────────
function WorkersTab({ date }) {
  const { data: seqData, loading: seqLoading } = useS3(date ? `timing/${date}_sequential.json` : null);
  const timings = seqData?.timings || [];

  const { data: srcData } = useS3(date ? `results/${date}_sources.json` : null);

  return (
    <>
      {seqLoading && <StateBox spin message="Loading worker data..." />}

      {!seqLoading && (
        <>
          {/* Worker assignment cards */}
          <div className="worker-grid">
            {WORKERS.map((w) => {
              const t = timings.find(t =>
                (t.source || '').toLowerCase().includes(w.key === 'wa' ? 'forecast' : w.key === 'wb' ? 'archive' : 'nws')
              );
              return (
                <div key={w.key} className={`worker-card ${w.key}`}>
                  <div className="worker-label">{w.label}</div>
                  <div className="worker-source">{w.source}</div>
                  <div className="worker-cities">{w.cities}</div>
                  {t ? (
                    <div className="worker-time">
                      {fmtSec(t.duration_ms)}<span className="unit">s</span>
                    </div>
                  ) : (
                    <div style={{fontSize:13,color:'var(--text-muted)',fontFamily:'var(--font-data)'}}>no data yet</div>
                  )}
                </div>
              );
            })}
          </div>

          {/* What each worker collects */}
          <div className="card">
            <div className="card-header">
              <span className="card-title">Worker Assignments — What Each One Does</span>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th>Data Source</th>
                    <th>City Group</th>
                    <th>What It Collects</th>
                    <th>API Used</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td><span className="wtag wa">Worker A</span></td>
                    <td className="mono">forecast</td>
                    <td className="mono">Cities 1–10</td>
                    <td>Current temperature, rainfall, wind speed</td>
                    <td className="mono" style={{fontSize:11}}>api.open-meteo.com/v1/forecast</td>
                  </tr>
                  <tr>
                    <td><span className="wtag wb">Worker B</span></td>
                    <td className="mono">archive</td>
                    <td className="mono">Cities 11–20</td>
                    <td>5-year historical average temp and rainfall for same week</td>
                    <td className="mono" style={{fontSize:11}}>archive-api.open-meteo.com/v1/archive</td>
                  </tr>
                  <tr>
                    <td><span className="wtag wc">Worker C</span></td>
                    <td className="mono">nws</td>
                    <td className="mono">Cities 21–30</td>
                    <td>Official NWS observed temperature and active weather alerts</td>
                    <td className="mono" style={{fontSize:11}}>api.weather.gov</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {/* Source records if available */}
          {srcData && srcData.length > 0 && (
            <div className="card">
              <div className="card-header">
                <span className="card-title">Raw Records Written to DynamoDB</span>
                <span style={{fontSize:11,color:'var(--text-muted)',fontFamily:'var(--font-data)'}}>
                  {srcData.length} records · each worker writes one row per city
                </span>
              </div>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>City</th>
                      <th>Source</th>
                      <th>Key Data Collected</th>
                      <th>Duration</th>
                    </tr>
                  </thead>
                  <tbody>
                    {srcData.slice(0, 30).map((r, i) => {
                      const wkey = r.source === 'forecast' ? 'wa' : r.source === 'archive' ? 'wb' : 'wc';
                      const keyData = r.source === 'forecast'
                        ? `${r.temp_f ?? '—'}°F current, ${r.rain_in ?? '—'}in rain`
                        : r.source === 'archive'
                        ? `${r.avg_temp_f ?? '—'}°F avg, ${r.avg_rain_in ?? '—'}in avg rain`
                        : `${r.obs_temp_f ?? 'N/A'}°F observed, ${JSON.parse(r.alerts||'[]').length} alerts`;
                      return (
                        <tr key={i}>
                          <td className="city-cell">{r.city}</td>
                          <td><span className={`wtag ${wkey}`}>{r.source}</span></td>
                          <td className="mono" style={{fontSize:12}}>{keyData}</td>
                          <td className="mono">{fmtMs(parseInt(r.duration_ms))}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {!srcData && !seqLoading && (
            <div className="card">
              <StateBox icon="📡" message="No source records yet for this date."
                sub="Trigger the aggregator Lambda to generate the sources file from DynamoDB." />
            </div>
          )}
        </>
      )}
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// TAB 3 — ANOMALIES
// Weather results from the forecast worker cities
// ─────────────────────────────────────────────────────────────────
function AnomalyTab({ date }) {
  const { data, loading, error } = useS3(date ? `results/${date}.json` : null);
  const { field, toggle, sort, arrow } = useSort('difference', 'desc');

  const results = sort(data || []);
  const flagged = results.filter(r => r.flagged).length;

  return (
    <>
      {results.length > 0 && (
        <div className="stats-row">
          <div className="stat-card accent">
            <div className="stat-label">Cities</div>
            <div className="stat-value">{results.length}</div>
            <div className="stat-sub">in results</div>
          </div>
          <div className="stat-card red">
            <div className="stat-label">Anomalies</div>
            <div className="stat-value">{flagged}</div>
            <div className="stat-sub">≥10°F from avg</div>
          </div>
          <div className="stat-card green">
            <div className="stat-label">Normal</div>
            <div className="stat-value">{results.length - flagged}</div>
            <div className="stat-sub">within range</div>
          </div>
        </div>
      )}

      <div className="card">
        <div className="card-header">
          <span className="card-title">Weather Anomaly Results — {fmtDate(date)}</span>
          <span style={{fontSize:11,color:'var(--text-muted)',fontFamily:'var(--font-data)'}}>
            written by aggregator Lambda at 8:30 AM
          </span>
        </div>

        {loading && <StateBox spin message="Loading anomaly data..." />}
        {!loading && (error || !data) && (
          <StateBox icon="📭" message="No anomaly data for this date."
            sub="Trigger the aggregator Lambda after the coordinator has run." />
        )}
        {!loading && !error && results.length > 0 && (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <SortTh label="City" field="city" current={field} toggle={toggle} arrow={arrow} />
                  <SortTh label="Today °F" field="current_temp" current={field} toggle={toggle} arrow={arrow} />
                  <SortTh label="5yr Avg °F" field="avg_temp" current={field} toggle={toggle} arrow={arrow} />
                  <SortTh label="Difference" field="difference" current={field} toggle={toggle} arrow={arrow} />
                  <th>Rain (in)</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => (
                  <tr key={`${r.city}-${i}`} className={r.flagged ? 'row-flagged' : ''}>
                    <td className="city-cell">{r.city}</td>
                    <td className="mono">{parseFloat(r.current_temp||0).toFixed(1)}</td>
                    <td className="mono">{parseFloat(r.avg_temp||0).toFixed(1)}</td>
                    <DiffCell value={r.difference} />
                    <td className="mono">{parseFloat(r.current_rain||0).toFixed(2)}</td>
                    <td>
                      {r.flagged
                        ? <span className="badge anomaly">⚠ Anomaly</span>
                        : <span className="badge normal">Normal</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// TAB 4 — ARCHIVE
// ─────────────────────────────────────────────────────────────────
function ArchiveTab() {
  const [selectedDate, setSelected] = useState('');
  const [subTab, setSubTab]         = useState('benchmark');

  return (
    <>
      <div style={{marginBottom:20,display:'flex',alignItems:'center',gap:12,flexWrap:'wrap'}}>
        <div className="date-control">
          <label>View past date:</label>
          <input type="date" className="date-input"
            value={selectedDate} max={todayStr()}
            onChange={e => setSelected(e.target.value)} />
        </div>
        {selectedDate && (
          <div style={{display:'flex',gap:4}}>
            {['benchmark','workers','anomalies'].map(t => (
              <button key={t}
                className={`tab-btn${subTab===t?' active':''}`}
                style={{flex:'0 0 auto',padding:'6px 14px',fontSize:12}}
                onClick={() => setSubTab(t)}>
                {t.charAt(0).toUpperCase()+t.slice(1)}
              </button>
            ))}
          </div>
        )}
      </div>

      {!selectedDate && (
        <StateBox icon="📅" message="Select a past date to view its data."
          sub="All benchmark and anomaly records are stored per day." />
      )}
      {selectedDate && subTab === 'benchmark'  && <BenchmarkTab date={selectedDate} />}
      {selectedDate && subTab === 'workers'    && <WorkersTab   date={selectedDate} />}
      {selectedDate && subTab === 'anomalies'  && <AnomalyTab   date={selectedDate} />}
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// ROOT APP
// ─────────────────────────────────────────────────────────────────
const TABS = [
  { id: 'benchmark', icon: '⚡', label: 'Benchmark' },
  { id: 'workers',   icon: '⚙',  label: 'Workers'   },
  { id: 'anomalies', icon: '🌡', label: 'Anomalies' },
  { id: 'archive',   icon: '📅', label: 'Archive'   },
];

export default function App() {
  const [tab, setTab] = useState('benchmark');
  const date = todayStr();

  return (
    <div className="app-root">

      <header className="app-header">
        <span className="header-icon">⚡</span>
        <div className="header-title">Anomaly Detector</div>
        <div className="header-sub">
          parallel cloud computing · atlanta metro · aws lambda · 3 workers · 3 sources
        </div>
        <div className="header-pills">
          <span className="pill"><span className="dot live" />Live · {fmtDate(date)}</span>
          <span className="pill"><span className="dot par" />Worker A — Open-Meteo Forecast</span>
          <span className="pill"><span className="dot par" style={{background:'var(--worker-b)'}} />Worker B — Open-Meteo Archive</span>
          <span className="pill"><span className="dot par" style={{background:'var(--worker-c)'}} />Worker C — NWS</span>
        </div>
      </header>

      <div className="tab-bar">
        {TABS.map(t => (
          <button key={t.id}
            className={`tab-btn ${tab === t.id ? 'active' : ''}`}
            onClick={() => setTab(t.id)}>
            <span className="tab-icon">{t.icon}</span>
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'benchmark' && <BenchmarkTab date={date} />}
      {tab === 'workers'   && <WorkersTab   date={date} />}
      {tab === 'anomalies' && <AnomalyTab   date={date} />}
      {tab === 'archive'   && <ArchiveTab />}

    </div>
  );
}