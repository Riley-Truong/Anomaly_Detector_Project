import { useEffect, useState } from 'react';
import axios from 'axios';
import './App.css';

const S3_BASE = 'https://anomaly-detector-dashboard.s3.us-east-2.amazonaws.com';

const WORKERS = [
  { id: 'A', label: 'Worker A', source: 'Open-Meteo Forecast', cities: 'Cities 1 – 10',  srcKey: 'forecast' },
  { id: 'B', label: 'Worker B', source: 'Open-Meteo Archive',  cities: 'Cities 11 – 20', srcKey: 'archive'  },
  { id: 'C', label: 'Worker C', source: 'NWS Observations',    cities: 'Cities 21 – 30', srcKey: 'nws'      },
];

const todayStr = () => new Date().toISOString().split('T')[0];

const fmtDate = (iso) => {
  if (!iso) return '';
  const [y, m, d] = iso.split('-');
  return new Date(y, m - 1, d).toLocaleDateString('en-US', {
    weekday: 'short', year: 'numeric', month: 'short', day: 'numeric',
  });
};

const fmtMs  = (ms) => ms == null || isNaN(ms) ? '—' : ms >= 1000 ? (ms / 1000).toFixed(2) + 's' : ms + 'ms';
const fmtSec = (ms) => ms == null || isNaN(ms) ? '—' : (ms / 1000).toFixed(2);

// ── S3 fetch hook ─────────────────────────────────────────────────
function useS3(key) {
  const [data,    setData]    = useState(null);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState(false);

  useEffect(() => {
    if (!key) return;
    axios.get(`${S3_BASE}/${key}`)
      .then(r  => { setData(r.data);   setLoading(false); })
      .catch(() => { setError(true);  setLoading(false); });
  }, [key]);

  return { data, loading, error };
}

// ── Sort hook ─────────────────────────────────────────────────────
function useSort(initField = 'difference', initDir = 'desc') {
  const [field, setField] = useState(initField);
  const [dir,   setDir]   = useState(initDir);

  const toggle = (f) => {
    setDir(prev => field === f ? (prev === 'asc' ? 'desc' : 'asc') : 'desc');
    setField(f);
  };

  const sort = (arr) => [...(arr || [])].sort((a, b) => {
    const va = a[field], vb = b[field];
    if (va == null) return 1;
    if (vb == null) return -1;
    const na = parseFloat(va), nb = parseFloat(vb);
    const cmp = !isNaN(na) && !isNaN(nb) ? na - nb
      : String(va).localeCompare(String(vb), undefined, { sensitivity: 'base' });
    return dir === 'asc' ? cmp : -cmp;
  });

  const arrow = (f) => (
    <span className="sort-icon" style={{ opacity: field === f ? 1 : 0.25 }}>
      {field === f ? (dir === 'asc' ? '↑' : '↓') : '↕'}
    </span>
  );

  return { field, toggle, sort, arrow };
}

// ── Shared primitives ─────────────────────────────────────────────
function Empty({ title, sub, loading }) {
  return (
    <div className="empty">
      {loading && <div className="spinner" />}
      <div className="empty-title">{title}</div>
      {sub && <div className="empty-sub">{sub}</div>}
    </div>
  );
}

function SortTh({ label, field, current, toggle, arrow }) {
  return (
    <th className={`sortable${current === field ? ' sort-active' : ''}`}
        onClick={() => toggle(field)}>
      {label}{arrow(field)}
    </th>
  );
}

function DiffCell({ value }) {
  const n = parseFloat(value);
  if (isNaN(n)) return <td className="neut num">—</td>;
  const cls = Math.abs(n) < 1 ? 'neut' : n > 0 ? 'pos' : 'neg';
  return <td className={`${cls} num`}>{n > 0 ? '+' : ''}{n.toFixed(1)}</td>;
}

// ─────────────────────────────────────────────────────────────────
// BENCHMARK TAB
// ─────────────────────────────────────────────────────────────────
function BenchmarkTab({ date }) {
  const { data: parData, loading: lp } = useS3(date ? `timing/${date}_parallel.json`   : null);
  const { data: seqData, loading: ls } = useS3(date ? `timing/${date}_sequential.json` : null);
  const loading = lp || ls;

  const parMs   = parData?.dispatch_ms ?? parData?.duration_ms ?? null;
  const seqMs   = seqData?.total_ms    ?? seqData?.duration_ms ?? null;
  const speedup = parMs && seqMs && parMs > 0 ? (seqMs / parMs).toFixed(2) : null;
  // const serial  = parMs && seqMs ? (parMs / seqMs * 100).toFixed(2) : null;
  // const maxTheo = serial ? (1 / (parseFloat(serial) / 100)).toFixed(1) : null;
  const parPct  = parMs && seqMs ? Math.max(1, (parMs / seqMs) * 100) : 1;
  const seqTimings = seqData?.timings || [];
  // const parTimings = parData?.timings || [];


  if (loading) return <Empty loading title="Loading benchmark data..." />;

  if (!speedup) return (
    <Empty title="No benchmark data for this date."
      sub={`Trigger the coordinator (parallel run) and sequential runner, then refresh.`} />
  );

  return (
    <>
      {/* Speedup */}
      <div className="speedup-row">
        <div className="speedup-main">
          <div className="speedup-eyebrow">Speedup Factor</div>
          <div className="speedup-figure">{speedup}<span className="unit">x</span></div>
          <div className="speedup-caption">parallel vs. sequential</div>
        </div>

        <div className="amdahl-card">
          <div className="amdahl-title">Analysis</div>
          <div className="amdahl-row">
            <span className="amdahl-row-label">Parallel time</span>
            <span className="amdahl-row-value">{fmtMs(parMs)}</span>
          </div>
          <div className="amdahl-row">
            <span className="amdahl-row-label">Sequential time</span>
            <span className="amdahl-row-value">{fmtMs(seqMs)}</span>
          </div>
          <div className="amdahl-row">
            <span className="amdahl-row-label">Measured speedup</span>
            <span className="amdahl-row-value">{speedup}x</span>
          </div>
        </div>
      </div>

      {/* Summary metrics */}
      <div className="metrics">
        <div className="metric blue">
          <div className="metric-label">Parallel</div>
          <div className="metric-value">{fmtSec(parMs)}</div>
          <div className="metric-sub">seconds</div>
        </div>
        <div className="metric amber">
          <div className="metric-label">Sequential</div>
          <div className="metric-value">{fmtSec(seqMs)}</div>
          <div className="metric-sub">seconds</div>
        </div>
        <div className="metric">
          <div className="metric-label">Speedup</div>
          <div className="metric-value">{speedup}x</div>
          <div className="metric-sub">faster</div>
        </div>
        <div className="metric">
          <div className="metric-label">Workers</div>
          <div className="metric-value">3</div>
          <div className="metric-sub">10 cities each</div>
        </div>
        <div className="metric">
          <div className="metric-label">Total cities</div>
          <div className="metric-value">30</div>
          <div className="metric-sub">Atlanta metro</div>
        </div>
      </div>

      {/* Time comparison bars */}
      <div className="card">
        <div className="card-head">
          <span className="card-title">Wall-Clock Time Comparison</span>
        </div>
        <div className="bars">
          <div className="bar-row">
            <div className="bar-top">
              <span className="bar-name">Parallel — all 3 workers fired simultaneously</span>
              <span className="bar-time">{fmtMs(parMs)}</span>
            </div>
            <div className="bar-track">
              <div className="bar-fill blue" style={{ width: `${parPct}%` }} />
            </div>
            <div className="bar-sub">
              Coordinator dispatched {parData?.workers ?? 3} workers via InvocationType=Event
            </div>
          </div>

          <div className="bar-row">
            <div className="bar-top">
              <span className="bar-name">Sequential — Worker A, then B, then C</span>
              <span className="bar-time">{fmtMs(seqMs)}</span>
            </div>
            <div className="bar-track">
              <div className="bar-fill amber" style={{ width: '100%' }} />
            </div>
            <div className="bar-sub">
              Each worker waited for the previous to finish via InvocationType=RequestResponse
            </div>
          </div>
        </div>
      </div>

      {/* Parallel breakdown
      {parTimings.length > 0 && (
        <div className="card">
          <div className="card-head">
            <span className="card-title">Parallel Execution Breakdown</span>
            <span className="card-hint">Worker A, B, C = total parallel time</span>
          </div>

          <div className="seq-timeline">
            <div className="seq-bar">
              {parTimings.map((t, i) => {
                const pct = parMs ? Math.max(1, (t.duration_ms / parMs) * 100) : 33;
                const cls = ['a', 'b', 'c'][i] || 'a';
                return <div key={i} className={`seq-seg ${cls}`} style={{ width: `${pct}%` }} />;
              })}
            </div>
            <div className="seq-legend">
              {parTimings.map((t, i) => {
                const cls = ['a', 'b', 'c'][i] || 'a';
                return (
                  <div key={i} className="legend-item">
                    <div className={`legend-dot ${cls}`} />
                    Worker {['A','B','C'][i]} ({t.source}) — {fmtMs(t.duration_ms)}
                  </div>
                );
              })}
            </div>
          </div>

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Worker</th>
                  <th>Source</th>
                  <th>Cities</th>
                  <th>Duration</th>
                  <th>% of Total</th>
                </tr>
              </thead>
              <tbody>
                {parTimings.map((t, i) => {
                  const pct = parMs ? ((t.duration_ms / parMs) * 100).toFixed(1) : '—';
                  return (
                    <tr key={i}>
                      <td className="city">Worker {['A','B','C'][i] || i+1}</td>
                      <td><span className="tag">{t.source}</span></td>
                      <td className="num">{t.cities ?? 10}</td>
                      <td className="num">{fmtMs(t.duration_ms)}</td>
                      <td className="num">{pct}%</td>
                    </tr>
                  );
                })}
                <tr className="total-row">
                  <td colSpan={3}>Total sequential</td>
                  <td className="num">{fmtMs(parMs)}</td>
                  <td className="num">100%</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )} */}


{/* Sequential breakdown */}
{seqTimings.length > 0 && (
  <div className="card">
    <div className="card-head">
      <span className="card-title">Sequential Execution Breakdown</span>
      <span className="card-hint">Worker A + B + C + overhead = total sequential time</span>
    </div>

    {(() => {
      const workerTotal = seqTimings.reduce(
        (sum, t) => sum + (t.duration_ms || 0),
        0
      );

      const overheadMs =
        seqMs && seqMs > workerTotal ? seqMs - workerTotal : 0;

      return (
        <>
          <div className="seq-timeline">
            <div className="seq-bar">
              {seqTimings.map((t, i) => {
                const pct = seqMs
                  ? (t.duration_ms / seqMs) * 100
                  : 0;

                const cls = ['a', 'b', 'c'][i] || 'a';

                return (
                  <div
                    key={i}
                    className={`seq-seg ${cls}`}
                    style={{ width: `${pct}%` }}
                  />
                );
              })}

              {overheadMs > 0 && (
                <div
                  className="seq-seg overhead"
                  style={{
                    width: `${(overheadMs / seqMs) * 100}%`
                  }}
                />
              )}
            </div>

            <div className="seq-legend">
              {seqTimings.map((t, i) => {
                const cls = ['a', 'b', 'c'][i] || 'a';

                return (
                  <div key={i} className="legend-item">
                    <div className={`legend-dot ${cls}`} />
                    Worker {['A', 'B', 'C'][i]} ({t.source}) — {fmtMs(t.duration_ms)}
                  </div>
                );
              })}

              {overheadMs > 0 && (
                <div className="legend-item">
                  <div className="legend-dot overhead" />
                  Overhead — {fmtMs(overheadMs)}
                </div>
              )}
            </div>
          </div>

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Worker</th>
                  <th>Source</th>
                  <th>Cities</th>
                  <th>Duration</th>
                  <th>% of Total</th>
                </tr>
              </thead>

              <tbody>
                {seqTimings.map((t, i) => {
                  const pct = seqMs
                    ? ((t.duration_ms / seqMs) * 100).toFixed(1)
                    : '—';

                  return (
                    <tr key={i}>
                      <td className="city">
                        Worker {['A', 'B', 'C'][i] || i + 1}
                      </td>
                      <td>
                        <span className="tag">{t.source}</span>
                      </td>
                      <td className="num">{t.cities ?? 10}</td>
                      <td className="num">{fmtMs(t.duration_ms)}</td>
                      <td className="num">{pct}%</td>
                    </tr>
                  );
                })}

                {overheadMs > 0 && (
                  <tr>
                    <td className="city">Overhead</td>
                    <td>
                      <span className="tag">system</span>
                    </td>
                    <td className="num">—</td>
                    <td className="num">{fmtMs(overheadMs)}</td>
                    <td className="num">
                      {((overheadMs / seqMs) * 100).toFixed(1)}%
                    </td>
                  </tr>
                )}

                <tr className="total-row">
                  <td colSpan={3}>Total sequential</td>
                  <td className="num">{fmtMs(seqMs)}</td>
                  <td className="num">100%</td>
                </tr>
              </tbody>
            </table>
          </div>
        </>
      );
    })()}
  </div>
)}

      {/* Concept table */}
      <div className="card">
        <div className="card-head">
          <span className="card-title">Execution Model — Why Parallel Is Faster</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Mode</th>
                <th>Completes when</th>
                <th>Total time</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="city">Parallel</td>
                <td>Slowest worker finishes</td>
                <td className="num" style={{ color: 'var(--blue)', fontWeight: 600 }}>{fmtMs(parMs)}</td>
              </tr>
              <tr>
                <td className="city">Sequential</td>
                <td>All workers finish</td>
                <td className="num" style={{ color: 'var(--amber)', fontWeight: 600 }}>{fmtMs(seqMs)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// WORKERS TAB
// ─────────────────────────────────────────────────────────────────
function WorkersTab({ date }) {
  const { data: seqData, loading } = useS3(date ? `timing/${date}_sequential.json` : null);
  const { data: srcData  }         = useS3(date ? `results/${date}_sources.json`    : null);
  const timings = seqData?.timings || [];

  return (
    <>

      {loading && <Empty loading title="Loading worker data..." />}

      <div style={{
  background: 'var(--bg)',
  border: '1px solid var(--border)',
  borderRadius: 'var(--r)',
  padding: '12px 16px',
  marginBottom: 16,
  fontSize: 13,
  color: 'var(--text-2)',
  lineHeight: 1.6
}}>
  Each worker is assigned a fixed group of 10 cities and a specific data source.
  The timing shown below reflects how long each worker took in the
  <strong style={{ color: 'var(--amber)', marginLeft: 4, marginRight: 4 }}>
    sequential run
  </strong>
  — where workers executed one after another.
  In the parallel run, all three fired simultaneously and the total time
  was approximately equal to the slowest worker alone.
</div>

      <div className="worker-grid">
        {WORKERS.map((w) => {
          const t = timings.find(t => (t.source || '').toLowerCase().includes(w.srcKey));
          return (
            <div key={w.id} className="worker-card">
              <div className="worker-card-left">
                <div className="worker-id">Worker {w.id}</div>
                <div className="worker-name">{w.source}</div>
                <div className="worker-src">
                  {w.id === 'A' ? 'api.open-meteo.com/v1/forecast'
                  : w.id === 'B' ? 'archive-api.open-meteo.com/v1/archive'
                  : 'api.weather.gov'}
                </div>
                <div className="worker-tag">{w.cities}</div>
              </div>
              <div className="worker-card-right">
                {t
                  ? <>
                      <div className="worker-duration">
                        {fmtSec(t.duration_ms)}<span className="unit">s</span>
                      </div>
                      <div className="worker-seq-label">sequential</div>
                    </>
                  : <div className="worker-no-data">no data</div>}
              </div>
            </div>            
            );
        })}
      </div>

      <div className="card">
        <div className="card-head">
          <span className="card-title">Worker Assignments</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Worker</th>
                <th>Data source</th>
                <th>City group</th>
                <th>Records written</th>
                <th>Fields collected</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="city">Worker A</td>
                <td><span className="tag">forecast</span></td>
                <td className="num">Cities 1 – 10</td>
                <td className="num">10</td>
                <td>temp_f, rain_in, wind_mph</td>
              </tr>
              <tr>
                <td className="city">Worker B</td>
                <td><span className="tag">archive</span></td>
                <td className="num">Cities 11 – 20</td>
                <td className="num">10</td>
                <td>avg_temp_f, avg_rain_in (5-yr avg)</td>
              </tr>
              <tr>
                <td className="city">Worker C</td>
                <td><span className="tag">nws</span></td>
                <td className="num">Cities 21 – 30</td>
                <td className="num">10</td>
                <td>obs_temp_f, alerts, has_warning</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {srcData && srcData.length > 0 && (
        <div className="card">
          <div className="card-head">
            <span className="card-title">DynamoDB Records</span>
            <span className="card-hint">{srcData.length} records written today</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>City</th>
                  <th>Source</th>
                  <th>Key value</th>
                  <th>Duration</th>
                </tr>
              </thead>
              <tbody>
                {srcData.slice(0, 30).map((r, i) => {
                  const val = r.source === 'forecast'
                    ? `${r.temp_f ?? '—'}°F current`
                    : r.source === 'archive'
                    ? `${r.avg_temp_f ?? '—'}°F avg`
                    : `${r.obs_temp_f ?? 'N/A'}°F observed`;
                  return (
                    <tr key={i}>
                      <td className="city">{r.city}</td>
                      <td><span className="tag">{r.source}</span></td>
                      <td className="num">{val}</td>
                      <td className="num">{fmtMs(parseInt(r.duration_ms))}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!srcData && !loading && (
        <div className="card">
          <Empty title="No source records yet."
            sub="Trigger the aggregator Lambda after the coordinator run completes." />
        </div>
      )}
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// ANOMALIES TAB
// ─────────────────────────────────────────────────────────────────
function AnomalyTab({ date }) {
  const { data, loading, error } = useS3(date ? `results/${date}.json` : null);
  const { field, toggle, sort, arrow } = useSort('difference', 'desc');
  const results  = sort(data || []);
  const flagged  = results.filter(r => r.flagged).length;

  return (
    <>
      {results.length > 0 && (
        <div className="metrics" style={{ marginBottom: 16 }}>
          <div className="metric">
            <div className="metric-label">Cities</div>
            <div className="metric-value">{results.length}</div>
            <div className="metric-sub">in results</div>
          </div>
          <div className="metric" style={flagged > 0 ? {borderColor:'var(--red-border)',background:'var(--red-bg)'} : {}}>
            <div className="metric-label">Anomalies</div>
            <div className="metric-value" style={flagged > 0 ? {color:'var(--red)'} : {}}>{flagged}</div>
            <div className="metric-sub">deviation ≥10°F</div>
          </div>
          <div className="metric">
            <div className="metric-label">Normal</div>
            <div className="metric-value">{results.length - flagged}</div>
            <div className="metric-sub">within range</div>
          </div>
          <div className="metric">
            <div className="metric-label">Threshold</div>
            <div className="metric-value" style={{ fontSize: 22, paddingTop: 3 }}>10°F</div>
            <div className="metric-sub">from 5-yr avg</div>
          </div>
        </div>
      )}

      <div className="card">
        <div className="card-head">
          <span className="card-title">Weather Anomaly Results — {fmtDate(date)}</span>
          <span className="card-hint">written by aggregator Lambda at 8:30 AM</span>
        </div>

        {loading && <Empty loading title="Loading..." />}
        {!loading && (error || !data) && (
          <Empty title="No results for this date."
            sub="Trigger the aggregator Lambda after the coordinator run." />
        )}

        {!loading && !error && results.length > 0 && (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <SortTh label="City"        field="city"         current={field} toggle={toggle} arrow={arrow} />
                  <SortTh label="Today (°F)"  field="current_temp" current={field} toggle={toggle} arrow={arrow} />
                  <SortTh label="5-yr Avg"    field="avg_temp"     current={field} toggle={toggle} arrow={arrow} />
                  <SortTh label="Difference"  field="difference"   current={field} toggle={toggle} arrow={arrow} />
                  <th>Rain (in)</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => (
                  <tr key={`${r.city}-${i}`} className={r.flagged ? 'flagged' : ''}>
                    <td className="city">{r.city}</td>
                    <td className="num">{parseFloat(r.current_temp || 0).toFixed(1)}</td>
                    <td className="num">{parseFloat(r.avg_temp || 0).toFixed(1)}</td>
                    <DiffCell value={r.difference} />
                    <td className="num">{parseFloat(r.current_rain || 0).toFixed(2)}</td>
                    <td>
                      {r.flagged
                        ? <span className="badge red">Anomaly</span>
                        : <span className="badge neutral">Normal</span>}
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
// ARCHIVE TAB
// ─────────────────────────────────────────────────────────────────
function ArchiveTab() {
  const [selDate, setSelDate] = useState('');
  const [subTab,  setSubTab]  = useState('benchmark');

  return (
    <>
      <div className="date-row">
        <span className="date-label">Select date</span>
        <input type="date" className="date-input"
          value={selDate} max={todayStr()}
          onChange={e => setSelDate(e.target.value)} />
        {selDate && (
          <div className="sub-tabs">
            {['benchmark', 'workers', 'single', 'anomalies'].map(t => (
              <button key={t}
                className={`sub-tab${subTab === t ? ' active' : ''}`}
                onClick={() => setSubTab(t)}>
                {t.charAt(0).toUpperCase() + t.slice(1)}
              </button>
            ))}
          </div>
        )}
      </div>

      {!selDate && (
        <Empty title="Select a date to view historical data."
          sub="Benchmark results, worker timing, and anomaly records are stored per day." />
      )}
      {selDate && subTab === 'benchmark' && <BenchmarkTab date={selDate} />}
      {selDate && subTab === 'workers'   && <WorkersTab   date={selDate} />}
      {selDate && subTab === 'anomalies' && <AnomalyTab   date={selDate} />}
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// SINGLE WORKER TAB
// One Lambda doing all the work vs 3 parallel workers
// ─────────────────────────────────────────────────────────────────
function SingleWorkerTab({ date }) {
  const { data: singleData, loading: sl } = useS3(date ? `timing/${date}_single.json`     : null);
  const { data: parData,    loading: pl } = useS3(date ? `timing/${date}_parallel.json`   : null);
  const { data: seqData,    loading: ql } = useS3(date ? `timing/${date}_sequential.json` : null);
  const loading = sl || pl || ql;

  const unwrap = (v) => {
    if (v == null) return null;
    if (typeof v === 'object' && 'S' in v) return v.S;
    if (typeof v === 'object' && 'N' in v) return v.N;
    return v;
  };

  const singleMs = parseFloat(unwrap(singleData?.total_ms) ?? unwrap(singleData?.duration_ms)) || null;
  const parMs    = parseFloat(unwrap(parData?.dispatch_ms) ?? unwrap(parData?.duration_ms))    || null;
  const seqMs    = parseFloat(unwrap(seqData?.total_ms)    ?? unwrap(seqData?.duration_ms))    || null;

  const timings  = singleData?.timings || [];

  // Speedup: single vs parallel
  const vsParallel   = singleMs && parMs  ? (singleMs / parMs).toFixed(2)  : null;
  // Speedup: single vs sequential
  const vsSequential = singleMs && seqMs  ? (singleMs / seqMs).toFixed(2)  : null;

  if (loading) return <Empty loading title="Loading single worker data..." />;

  if (!singleMs) return (
    <Empty title="No single worker data for this date."
      sub="Trigger AnomalyDetectorWorkerSingle then refresh." />
  );

  return (
    <>
      {/* What this is */}
      <div style={{
        background: 'var(--bg)', border: '1px solid var(--border)',
        borderRadius: 'var(--r)', padding: '12px 16px',
        marginBottom: 16, fontSize: 13, color: 'var(--text-2)', lineHeight: 1.6,
      }}>
        A single Lambda function processes all 30 cities alone — forecast for cities 1–10,
        archive for 11–20, and NWS for 21–30 — switching between APIs sequentially.
        This is compared against the 3-worker parallel run and the 3-worker sequential run.
      </div>

      {/* Three-way metric comparison */}
      <div className="metrics">
        <div className="metric">
          <div className="metric-label">Single worker</div>
          <div className="metric-value">{fmtSec(singleMs)}</div>
          <div className="metric-sub">seconds total</div>
        </div>
        <div className="metric amber">
          <div className="metric-label">Sequential (3 workers)</div>
          <div className="metric-value">{fmtSec(seqMs) ?? '—'}</div>
          <div className="metric-sub">seconds total</div>
        </div>
        <div className="metric blue">
          <div className="metric-label">Parallel (3 workers)</div>
          <div className="metric-value">{fmtSec(parMs) ?? '—'}</div>
          <div className="metric-sub">seconds dispatch</div>
        </div>
        {vsSequential && (
          <div className="metric">
            <div className="metric-label">vs Sequential</div>
            <div className="metric-value">{parseFloat(vsSequential) > 1 ? vsSequential + 'x slower' : (1/parseFloat(vsSequential)).toFixed(2) + 'x faster'}</div>
            <div className="metric-sub">single vs 3-worker seq</div>
          </div>
        )}
        {vsParallel && (
          <div className="metric">
            <div className="metric-label">vs Parallel</div>
            <div className="metric-value">{vsParallel}x</div>
            <div className="metric-sub">parallel advantage</div>
          </div>
        )}
      </div>

      {/* Single worker segment breakdown */}
      {timings.length > 0 && (
        <div className="card">
          <div className="card-head">
            <span className="card-title">Single Worker — Segment Breakdown</span>
            <span className="card-hint">Phase A + B + C + overhead = total single worker time</span>
          </div>

          {(() => {
            const workerTotal = timings.reduce(
              (sum, t) => sum + (t.duration_ms || 0),
              0
            );

            const overheadMs =
              singleMs && singleMs > workerTotal ? singleMs - workerTotal : 0;

            return (
              <>
                <div className="seq-timeline" style={{ padding: 16 }}>
                  <div className="seq-bar">
                    {timings.map((t, i) => {
                      const pct = singleMs
                        ? (t.duration_ms / singleMs) * 100
                        : 0;
                      const cls = ['a', 'b', 'c'][i] || 'a';
                      return (
                        <div
                          key={i}
                          className={`seq-seg ${cls}`}
                          style={{ width: `${pct}%` }}
                        />
                      );
                    })}

                    {overheadMs > 0 && (
                      <div
                        className="seq-seg overhead"
                        style={{ width: `${(overheadMs / singleMs) * 100}%` }}
                      />
                    )}
                  </div>

                  <div className="seq-legend">
                    {timings.map((t, i) => {
                      const cls = ['a', 'b', 'c'][i] || 'a';
                      return (
                        <div key={i} className="legend-item">
                          <div className={`legend-dot ${cls}`} />
                          Phase {['A', 'B', 'C'][i]} ({t.source}) — {fmtMs(t.duration_ms)}
                        </div>
                      );
                    })}

                    {overheadMs > 0 && (
                      <div className="legend-item">
                        <div className="legend-dot overhead" />
                        Overhead — {fmtMs(overheadMs)}
                      </div>
                    )}
                  </div>
                </div>

                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Phase</th>
                        <th>Source</th>
                        <th>Cities</th>
                        <th>Duration</th>
                        <th>% of Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {timings.map((t, i) => {
                        const pct = singleMs
                          ? ((t.duration_ms / singleMs) * 100).toFixed(1)
                          : '—';
                        return (
                          <tr key={i}>
                            <td className="city">Phase {['A', 'B', 'C'][i] || i + 1}</td>
                            <td><span className="tag">{t.source}</span></td>
                            <td className="num">{t.cities}</td>
                            <td className="num">{fmtMs(t.duration_ms)}</td>
                            <td className="num">{pct}%</td>
                          </tr>
                        );
                      })}

                      {overheadMs > 0 && (
                        <tr>
                          <td className="city">Overhead</td>
                          <td><span className="tag">system</span></td>
                          <td className="num">—</td>
                          <td className="num">{fmtMs(overheadMs)}</td>
                          <td className="num">
                            {((overheadMs / singleMs) * 100).toFixed(1)}%
                          </td>
                        </tr>
                      )}

                      <tr className="total-row">
                        <td colSpan={3}>Total single worker</td>
                        <td className="num">{fmtMs(singleMs)}</td>
                        <td className="num">100%</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </>
            );
          })()}
        </div>
      )}


      {/* What this tells you */}
      <div className="card">
        <div className="card-head">
          <span className="card-title">What This Comparison Tells You</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Model</th>
                <th>Workers</th>
                <th>Execution</th>
                <th>Time</th>
                <th>Key insight</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="city">Single worker</td>
                <td className="num">1</td>
                <td>All sources, one function</td>
                <td className="num">{fmtMs(singleMs)}</td>
                <td>Baseline — no parallelism, no specialization</td>
              </tr>
              <tr>
                <td className="city">Sequential</td>
                <td className="num">3</td>
                <td>Specialized, one at a time</td>
                <td className="num">{fmtMs(seqMs) ?? '—'}</td>
                <td>Specialization benefit without parallelism</td>
              </tr>
              <tr>
                <td className="city">Parallel</td>
                <td className="num">3</td>
                <td>Specialized, simultaneous</td>
                <td className="num">{fmtMs(parMs) ?? '—'}</td>
                <td>Full benefit — specialization + parallelism</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

// ─────────────────────────────────────────────────────────────────
// ROOT
// ─────────────────────────────────────────────────────────────────
// Icons for mobile nav — simple text characters, no emojis
const TAB_ICONS = {
  benchmark: '≋',
  workers:   '⊞',
  single:    '◈',
  anomalies: '△',
  archive:   '◷',
};

const TABS = [
  { id: 'benchmark',    label: 'Benchmark'     },
  { id: 'workers',      label: 'Workers'       },
  { id: 'single',       label: 'Single Worker' },
  { id: 'anomalies',    label: 'Anomalies'     },
  { id: 'archive',      label: 'Archive'       },
];

export default function App() {
  const [tab,      setTab]      = useState('benchmark');
  const [dark,     setDark]     = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const date = todayStr();

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
  }, [dark]);

  // Close menu when tab changes
  const handleTabChange = (id) => {
    setTab(id);
    setMenuOpen(false);
  };

  const tabTitles = {
    benchmark: { title: 'Parallel vs. Sequential Benchmark', desc: 'Measuring the speedup of running 3 workers simultaneously versus sequentially.' },
    workers:   { title: 'Worker Assignments',                desc: '30 cities split across 3 workers, each calling a different data source. Sequential timing shown per worker.' },
    single: { title: 'Single Worker Analysis', desc: 'One Lambda processes all 30 cities sequentially across 3 different data sources.' },
    anomalies: { title: 'Weather Anomaly Results',           desc: 'Cities deviating more than 10°F from their 5-year historical average for the same week.' },
    archive:   { title: 'Historical Records',                desc: 'Browse benchmark and anomaly data from previous days.' },
  };

  return (
    <div className="app" style={{ position: 'relative' }}>
      <header className="topbar">
        <div className="topbar-left">
          <div className="brand">
            Anomaly Detector
          </div>

          {/* Desktop nav — visible at 900px+ */}
          <nav className="nav">
            {TABS.map(t => (
              <button key={t.id}
                className={`nav-btn${tab === t.id ? ' active' : ''}`}
                onClick={() => handleTabChange(t.id)}>
                {t.label}
              </button>
            ))}
          </nav>

          {/* Hamburger — visible between 600px and 899px */}
          <button
            className="hamburger"
            onClick={() => setMenuOpen(o => !o)}
            aria-label="Toggle navigation"
          >
            <span className="hamburger-line" />
            <span className="hamburger-line" />
            <span className="hamburger-line" />
          </button>
        </div>

        <div className="topbar-right">
          <button className="theme-btn" onClick={() => setDark(d => !d)}>
            {dark ? 'Light' : 'Dark'}
          </button>
          <span className="live-badge">
            <span className="live-dot" />
            Live
          </span>
          <span className="date-chip">{date}</span>
        </div>
      </header>

      {/* Dropdown menu — only renders when open */}
      {menuOpen && (
        <div className="nav-dropdown">
          {TABS.map(t => (
            <button key={t.id}
              className={`nav-btn${tab === t.id ? ' active' : ''}`}
              onClick={() => handleTabChange(t.id)}>
              {t.label}
            </button>
          ))}
        </div>
      )}

      <main className="page">
        <div className="page-header">
          <div className="page-title">{tabTitles[tab].title}</div>
          <div className="page-desc">{tabTitles[tab].desc}</div>
        </div>

        {tab === 'benchmark' && <BenchmarkTab date={date} />}
        {tab === 'workers'   && <WorkersTab   date={date} />}
        {tab === 'single'    && <SingleWorkerTab date={date} />}
        {tab === 'anomalies' && <AnomalyTab   date={date} />}
        {tab === 'archive'   && <ArchiveTab />}
      </main>

      {/* Mobile bottom nav — visible below 600px */}
      <nav className="mobile-nav">
        {TABS.map(t => (
          <button key={t.id}
            className={`mobile-nav-btn${tab === t.id ? ' active' : ''}`}
            onClick={() => handleTabChange(t.id)}>
            <span className="mobile-nav-icon">{TAB_ICONS[t.id]}</span>
            {t.label}
          </button>
        ))}
      </nav>
    </div>
  );
}