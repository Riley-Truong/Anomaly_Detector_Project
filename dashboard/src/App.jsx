import { useEffect, useState, useCallback } from 'react';
import axios from 'axios';
import './App.css';

const S3_BASE = 'https://anomaly-detector-dashboard.s3.us-east-2.amazonaws.com';

const today = () => new Date().toISOString().split('T')[0];

const formatDate = (isoStr) => {
  if (!isoStr) return '';
  const [y, m, d] = isoStr.split('-');
  return new Date(y, m - 1, d).toLocaleDateString('en-US', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  });
};

function App() {
  const [tab, setTab] = useState('today');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [sortField, setSortField] = useState('difference');
  const [sortDir, setSortDir] = useState('desc');
  const [archiveDate, setArchiveDate] = useState('');
  const [activeDate, setActiveDate] = useState(today());

  const fetchData = useCallback((dateStr) => {
    setLoading(true);
    setError(false);
    setResults([]);
    axios.get(`${S3_BASE}/results/${dateStr}.json`)
      .then(res => {
        setResults(Array.isArray(res.data) ? res.data : []);
        setLoading(false);
      })
      .catch(() => {
        setError(true);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    fetchData(today());
  }, [fetchData]);

  const switchTab = (t) => {
    setTab(t);
    if (t === 'today') {
      setActiveDate(today());
      fetchData(today());
    } else {
      setResults([]);
      setLoading(false);
      setError(false);
    }
  };

  const handleArchiveDateChange = (e) => {
    const d = e.target.value;
    setArchiveDate(d);
    if (d) {
      setActiveDate(d);
      fetchData(d);
    }
  };

  const handleSort = (field) => {
    setSortDir(prev => (sortField === field ? (prev === 'asc' ? 'desc' : 'asc') : 'asc'));
    setSortField(field);
  };

  const sorted = [...results].sort((a, b) => {
    const rawA = a[sortField];
    const rawB = b[sortField];

    if (rawA == null) return 1;
    if (rawB == null) return -1;

    const numA = parseFloat(rawA);
    const numB = parseFloat(rawB);
    const numeric = !isNaN(numA) && !isNaN(numB);

    const cmp = numeric
      ? numA - numB
      : String(rawA).localeCompare(String(rawB), undefined, { sensitivity: 'base' });

    return sortDir === 'asc' ? cmp : -cmp;
  });

  const flaggedCount = results.filter(r => r.flagged).length;
  const normalCount  = results.length - flaggedCount;

  const sortArrow = (field) => {
    if (sortField !== field) return <span className="sort-icon" style={{ opacity: 0.3 }}>↕</span>;
    return <span className="sort-icon">{sortDir === 'asc' ? '↑' : '↓'}</span>;
  };

  const diffClass = (val) => {
    const n = parseFloat(val);
    if (isNaN(n) || Math.abs(n) < 1) return 'diff-neutral';
    return n > 0 ? 'diff-positive' : 'diff-negative';
  };

  const diffLabel = (val) => {
    const n = parseFloat(val);
    if (isNaN(n)) return '—';
    return (n > 0 ? '+' : '') + n.toFixed(1);
  };

  const rowClass = (r, i) => {
    if (r.flagged) return 'row-flagged';
    return i % 2 === 0 ? 'row-even' : 'row-odd';
  };

  return (
    <>
      <div className="header">
        <div className="app-wrapper">
          <div className="header-top">
            <div>
              <div className="header-title">Anomaly Detector</div>
              <div className="header-subtitle">
                Parallel weather anomaly tracking — Atlanta metro area
              </div>
            </div>
            <div className="header-badge">
              <span className="dot" />
              {tab === 'today' ? 'Live — ' + formatDate(today()) : 'Archive — ' + formatDate(activeDate)}
            </div>
          </div>
        </div>
      </div>

      <div className="app-wrapper">

        {results.length > 0 && (
          <div className="stats-row">
            <div className="stat-card">
              <div className="stat-label">Cities checked</div>
              <div className="stat-value total">{results.length}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Flagged anomalies</div>
              <div className="stat-value flagged">{flaggedCount}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Normal readings</div>
              <div className="stat-value normal">{normalCount}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Threshold</div>
              <div className="stat-value" style={{ fontSize: '20px', color: 'var(--gray-700)' }}>±10°F</div>
            </div>
          </div>
        )}

        <div className="tabs">
          <button
            className={`tab-btn ${tab === 'today' ? 'active' : ''}`}
            onClick={() => switchTab('today')}
          >
            Today
          </button>
          <button
            className={`tab-btn ${tab === 'archive' ? 'active' : ''}`}
            onClick={() => switchTab('archive')}
          >
            Archive
          </button>
        </div>

        <div className="card">
          <div className="card-header">
            <span className="card-title">
              {tab === 'today'
                ? `Results for ${formatDate(today())}`
                : archiveDate
                  ? `Results for ${formatDate(archiveDate)}`
                  : 'Select a date to view past data'}
            </span>

            {tab === 'archive' && (
              <div className="archive-controls">
                <label htmlFor="archive-date">View date:</label>
                <input
                  id="archive-date"
                  type="date"
                  className="date-input"
                  value={archiveDate}
                  max={today()}
                  onChange={handleArchiveDateChange}
                />
              </div>
            )}
          </div>

          {tab === 'archive' && !archiveDate && (
            <div className="state-box">
              <span className="state-icon">📅</span>
              <p>Pick a date above to load that day's anomaly data.</p>
              <p className="state-sub">Data is available for every day the system has run.</p>
            </div>
          )}

          {loading && (tab === 'today' || archiveDate) && (
            <div className="state-box">
              <div className="spinner" />
              <p>Loading data...</p>
            </div>
          )}

          {!loading && error && (
            <div className="state-box">
              <span className="state-icon">🔍</span>
              <p>No data found for this date.</p>
              <p className="state-sub">
                {tab === 'today'
                  ? 'The daily run may not have completed yet. Check back after 8 AM EST.'
                  : 'The system may not have been running on this date, or the file does not exist in S3 yet.'}
              </p>
            </div>
          )}

          {!loading && !error && results.length > 0 && (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th
                      className={`sortable ${sortField === 'city' ? 'sort-active' : ''}`}
                      onClick={() => handleSort('city')}
                    >
                      City {sortArrow('city')}
                    </th>
                    <th
                      className={`sortable ${sortField === 'current_temp' ? 'sort-active' : ''}`}
                      onClick={() => handleSort('current_temp')}
                    >
                      Today (°F) {sortArrow('current_temp')}
                    </th>
                    <th
                      className={`sortable ${sortField === 'avg_temp' ? 'sort-active' : ''}`}
                      onClick={() => handleSort('avg_temp')}
                    >
                      5-yr Avg (°F) {sortArrow('avg_temp')}
                    </th>
                    <th
                      className={`sortable ${sortField === 'difference' ? 'sort-active' : ''}`}
                      onClick={() => handleSort('difference')}
                    >
                      Difference {sortArrow('difference')}
                    </th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {sorted.map((r, i) => (
                    <tr key={`${r.city}-${i}`} className={rowClass(r, i)}>
                      <td className="city-cell">{r.city}</td>
                      <td>{parseFloat(r.current_temp).toFixed(1)}</td>
                      <td>{parseFloat(r.avg_temp).toFixed(1)}</td>
                      <td className={diffClass(r.difference)}>
                        {diffLabel(r.difference)}
                      </td>
                      <td className="flag-cell">
                        {r.flagged
                          ? <span className="flag-badge"><span className="flag-icon">⚠</span></span>
                          : <span style={{ color: 'var(--gray-400)', fontSize: '13px' }}>Normal</span>
                        }
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

export default App;