import { useEffect, useState } from 'react'
import './App.css'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'
const PAGE_SIZE = 100

function App() {
  const [query, setQuery] = useState('')
  const [submittedQuery, setSubmittedQuery] = useState('')
  const [rows, setRows] = useState([])
  const [fetchedCount, setFetchedCount] = useState(0)
  const [scannedCount, setScannedCount] = useState(0)
  const [savedCount, setSavedCount] = useState(0)
  const [fatsecretTotalReported, setFatsecretTotalReported] = useState(0)
  const [dbTotalAfterSave, setDbTotalAfterSave] = useState(0)
  const [crawlLabel, setCrawlLabel] = useState('')
  const [progressPercent, setProgressPercent] = useState(0)
  const [downloadedMb, setDownloadedMb] = useState(0)
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const [totalCount, setTotalCount] = useState(0)
  const [isFetching, setIsFetching] = useState(false)
  const [isSearching, setIsSearching] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [dbTableName, setDbTableName] = useState('fatsecret_foods')

  const loadSavedData = async (searchText, page = 1, showMessage = true) => {
    const normalized = (searchText || '').trim()
    setIsSearching(true)
    setError('')
    setMessage('')

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/fatsecret/saved?q=${encodeURIComponent(normalized)}&page=${page}&page_size=${PAGE_SIZE}`
      )
      const data = await response.json()
      if (!response.ok) {
        throw new Error(data?.detail || 'Failed to load data from DB.')
      }

      setRows(Array.isArray(data.foods) ? data.foods : [])
      setSubmittedQuery(normalized || 'All Saved Data')
      setCurrentPage(Number(data.page || page))
      setTotalPages(Number(data.total_pages || 1))
      setTotalCount(Number(data.total_count || 0))
      if (data.db_table) setDbTableName(data.db_table)
      if (showMessage) {
        setMessage(
          `Loaded ${data.count ?? 0} rows (page ${data.page ?? page}/${data.total_pages ?? 1}) from DB table "${data.db_table}".`
        )
      }
    } catch (requestError) {
      setRows([])
      setError(requestError.message || 'Unexpected error.')
    } finally {
      setIsSearching(false)
    }
  }

  const handleFetchAndSave = async () => {
    const cleanQuery = query.trim()
    const useCrawlAll = cleanQuery.length < 2
    setIsFetching(true)
    setError('')
    setMessage('')
    setProgressPercent(0)
    setDownloadedMb(0)
    setFetchedCount(0)
    setScannedCount(0)
    setSavedCount(0)
    setFatsecretTotalReported(0)
    setDbTotalAfterSave(0)
    setCrawlLabel('')

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/fatsecret/search/stream?q=${encodeURIComponent(cleanQuery)}&max_results=50&fetch_all=true&crawl_all=${useCrawlAll}&crawl_pages_per_term=3&max_total=35000&save_to_db=true&reset_db=true`
      )
      if (!response.ok) {
        throw new Error('Failed to fetch from FatSecret.')
      }
      if (!response.body) {
        throw new Error('Streaming not supported in this browser.')
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let completedEvent = null

      while (true) {
        const { value, done } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const rawLine of lines) {
          const line = rawLine.trim()
          if (!line) continue
          const event = JSON.parse(line)

          if (event.type === 'progress') {
            if (event.mode === 'crawl_all') {
              setSubmittedQuery('All Terms Crawl')
              const term = event.term || ''
              const termIndex = Number(event.term_index || 0)
              const termsTotal = Number(event.terms_total || 0)
              setCrawlLabel(term ? `Term ${term} (${termIndex}/${termsTotal})` : '')
            } else {
              setSubmittedQuery(event.query || cleanQuery || 'a')
              setCrawlLabel('')
            }
            setFetchedCount(event.fetched_count ?? 0)
            setScannedCount(Number(event.scanned_count || 0))
            setSavedCount(event.saved_count ?? 0)
            setFatsecretTotalReported(Number(event.fatsecret_total_reported || 0))
            setProgressPercent(Number(event.progress_percent || 0))
            setDownloadedMb(Number(event.downloaded_mb || 0))
            if (event.db_error) {
              setError(`DB save issue: ${event.db_error}`)
            }
            continue
          }

          if (event.type === 'complete') {
            completedEvent = event
            continue
          }

          if (event.type === 'error') {
            throw new Error(event.detail || 'Stream fetch failed.')
          }
        }
      }

      if (!completedEvent) {
        throw new Error('Fetch stream ended unexpectedly.')
      }

      setFetchedCount(completedEvent.fetched_count ?? 0)
      setScannedCount(Number(completedEvent.scanned_count || 0))
      setSavedCount(completedEvent.saved_count ?? 0)
      setFatsecretTotalReported(Number(completedEvent.fatsecret_total_reported || completedEvent.total_results || 0))
      setDbTotalAfterSave(Number(completedEvent.db_total_count || 0))
      setProgressPercent(Number(completedEvent.progress_percent || 100))
      setDownloadedMb(Number(completedEvent.downloaded_mb || 0))
      if (completedEvent.db_table) setDbTableName(completedEvent.db_table)
      if (completedEvent.mode === 'crawl_all') {
        setSubmittedQuery('All Terms Crawl')
        setCrawlLabel('')
      } else {
        setSubmittedQuery(completedEvent.query || cleanQuery || 'a')
        setCrawlLabel('')
      }

      if (completedEvent.db_error) {
        throw new Error(`DB save issue: ${completedEvent.db_error}`)
      }

      await loadSavedData(query, 1, false)
      const reportedTotal = Number(completedEvent.fatsecret_total_reported || completedEvent.total_results || 0)
      const dbTotal = Number(completedEvent.db_total_count || 0)
      const savedRows = Number(completedEvent.saved_count || 0)
      const isCrawlAll = completedEvent.mode === 'crawl_all'
      const countsMatch = isCrawlAll ? savedRows === dbTotal : reportedTotal > 0 && reportedTotal === dbTotal
      if (completedEvent.mode === 'crawl_all') {
        setMessage(
          `FatSecret raw total ${reportedTotal || 'N/A'}, unique saved ${savedRows}, DB now has ${dbTotal} rows (${countsMatch ? 'match' : 'not match'}).`
        )
      } else {
        setMessage(
          `FatSecret total ${reportedTotal || 'N/A'}, saved ${savedRows}, DB now has ${dbTotal} rows (${countsMatch ? 'match' : 'not match'}).`
        )
      }
    } catch (requestError) {
      setError(requestError.message || 'Unexpected error.')
    } finally {
      setIsFetching(false)
    }
  }

  const macroText = (row) => {
    if (row.macros_g) return row.macros_g
    const parts = []
    if (row.fat_g) parts.push(`Fat ${row.fat_g}g`)
    if (row.carbs_g) parts.push(`Carbs ${row.carbs_g}g`)
    if (row.protein_g) parts.push(`Protein ${row.protein_g}g`)
    return parts.join(' | ')
  }

  const handlePrevPage = async () => {
    if (currentPage <= 1 || isFetching || isSearching) return
    await loadSavedData(query, currentPage - 1, false)
  }

  const handleNextPage = async () => {
    if (currentPage >= totalPages || isFetching || isSearching) return
    await loadSavedData(query, currentPage + 1, false)
  }

  useEffect(() => {
    if (isFetching) return
    loadSavedData(query, 1, false)
  }, [query, isFetching])

  return (
    <main className="app-shell">
      <section className="search-panel">
        <h1>FatSecret Food Search</h1>
        <form
          onSubmit={(event) => {
            event.preventDefault()
          }}
          className="search-form"
        >
          <input
            type="search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search from saved DB data"
          />
          <button
            type="button"
            onClick={handleFetchAndSave}
            disabled={isFetching || isSearching}
            className="secondary-btn"
          >
            {isFetching ? 'Fetching...' : 'Fetch FatSecret Data'}
          </button>
        </form>

        {isFetching ? (
          <p className="progress-message">
            Fetching: {progressPercent.toFixed(2)}% | {downloadedMb.toFixed(2)} MB | Scanned: {scannedCount} | Saved:{' '}
            {savedCount}
            {crawlLabel ? ` | ${crawlLabel}` : ''}
          </p>
        ) : null}
        {message ? <p className="success-message">{message}</p> : null}
        {error ? <p className="error-message">{error}</p> : null}
      </section>

      <section className="results-panel">
        <h2>Food Data</h2>
        <p className="query-text">
          Source: <strong>{dbTableName}</strong>
          {submittedQuery ? ` | Search: ${submittedQuery}` : ''}
          {fatsecretTotalReported ? ` | FatSecret Total: ${fatsecretTotalReported}` : ''}
          {fetchedCount ? ` | Fetched: ${fetchedCount}` : ''}
          {scannedCount ? ` | Scanned: ${scannedCount}` : ''}
          {savedCount ? ` | Saved: ${savedCount}` : ''}
          {dbTotalAfterSave ? ` | DB Total: ${dbTotalAfterSave}` : ''}
          {` | Showing: ${rows.length}`}
          {` | Total: ${totalCount}`}
        </p>

        <div className="table-wrap">
          <table className="results-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Brand</th>
                <th>Category</th>
                <th>Calories / serving</th>
                <th>Macros (g)</th>
                <th>Status</th>
                <th>Source</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.length > 0 ? (
                rows.map((row, index) => (
                  <tr key={`${row.food_id || row.id || index}`}>
                    <td>
                      <strong>{row.food_name || '-'}</strong>
                      <div className="sub-text">{row.food_description || ''}</div>
                    </td>
                    <td>{row.brand_name || '-'}</td>
                    <td>{row.category || row.food_type || '-'}</td>
                    <td>{row.calories_per_serving || '-'}</td>
                    <td>{macroText(row) || '-'}</td>
                    <td>{row.status || 'Fetched'}</td>
                    <td>{row.source || 'FatSecret'}</td>
                    <td>
                      {row.food_url ? (
                        <a href={row.food_url} target="_blank" rel="noreferrer" className="action-link">
                          Open
                        </a>
                      ) : (
                        '-'
                      )}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={8} className="empty-row">
                    {isFetching || isSearching ? 'Loading...' : 'No data found in DB.'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="pagination-bar">
          <button type="button" onClick={handlePrevPage} disabled={currentPage <= 1 || isFetching || isSearching}>
            Prev
          </button>
          <span>
            Page {currentPage} / {totalPages}
          </span>
          <button
            type="button"
            onClick={handleNextPage}
            disabled={currentPage >= totalPages || isFetching || isSearching}
          >
            Next
          </button>
        </div>
      </section>
    </main>
  )
}

export default App
