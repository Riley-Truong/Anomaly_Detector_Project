import React, { useEffect, useState } from 'react';
import axios from 'axios';
 
// Replace this with your actual S3 bucket URL
const S3_BASE = 'https://anomaly-detector-dashboard.s3.amazonaws.com';
 
function App() {
  const [results, setResults]   = useState([]);
  const [loading, setLoading]   = useState(true);
  const [sortField, setSortField] = useState('difference');
  const [sortDir, setSortDir]   = useState('desc');
 
  useEffect(() => {
    const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
    axios.get(`${S3_BASE}/results/${today}.json`)
      .then(res => {
        setResults(res.data);
        setLoading(false);
      })
      .catch(() => {
        setLoading(false);  // No data yet for today
      });
  }, []);
 
  const sorted = [...results].sort((a, b) => {
    const val = sortDir === 'desc'
      ? b[sortField] - a[sortField]
      : a[sortField] - b[sortField];
    return val;
  });
 
  const handleSort = (field) => {
    if (sortField === field) setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    else { setSortField(field); setSortDir('desc'); }
  };
 
  return (
    <div style={{ fontFamily: 'Arial, sans-serif', padding: '20px' }}>
      <h1 style={{ color: '#1F3864' }}>AD Weather Anomaly Dashboard</h1>
      <p style={{ color: '#595959' }}>
        Showing anomalies for {new Date().toDateString()}.
        Cities more than 10F from historical average are highlighted.
      </p>
 
      {loading && <p>Loading data...</p>}
      {!loading && results.length === 0 && (
        <p>No data yet for today. Check back after 8 AM Eastern.</p>
      )}
 
      {results.length > 0 && (
        <table style={{ borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr style={{ backgroundColor: '#2E74B5', color: 'white' }}>
              <th style={th}>City</th>
              <th style={th} onClick={() => handleSort('current_temp')}>
                Today (F) {sortField === 'current_temp' ? arrow(sortDir) : ''}
              </th>
              <th style={th} onClick={() => handleSort('avg_temp')}>
                Avg (F) {sortField === 'avg_temp' ? arrow(sortDir) : ''}
              </th>
              <th style={th} onClick={() => handleSort('difference')}>
                Difference {sortField === 'difference' ? arrow(sortDir) : ''}
              </th>
              <th style={th}>Flagged</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={r.city}
                style={{ backgroundColor: r.flagged ? '#FFF0F0' : (i%2===0 ? '#fff' : '#f7f7f7') }}>
                <td style={td}>{r.city}</td>
                <td style={td}>{parseFloat(r.current_temp).toFixed(1)}</td>
                <td style={td}>{parseFloat(r.avg_temp).toFixed(1)}</td>
                <td style={{ ...td, fontWeight: r.flagged ? 'bold' : 'normal',
                              color: parseFloat(r.difference) > 0 ? '#CC0000' : '#005500' }}>
                  {parseFloat(r.difference) > 0 ? '+' : ''}
                  {parseFloat(r.difference).toFixed(1)}
                </td>
                <td style={{ ...td, textAlign: 'center' }}>
                  {r.flagged ? '🚨' : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
 
const th = { padding: '10px', textAlign: 'left', cursor: 'pointer', userSelect: 'none' };
const td = { padding: '8px 10px', borderBottom: '1px solid #eee' };
const arrow = dir => dir === 'desc' ? ' ▼' : ' ▲';
 
export default App;
