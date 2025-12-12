import React, { useState, useEffect, useCallback } from 'react'
import { useDropzone } from 'react-dropzone'
import axios from 'axios'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, BarChart, Bar } from 'recharts'

const API_BASE = '/api'

function FileUpload({ onFileUploaded }) {
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState(null)

  const onDrop = useCallback(async (acceptedFiles) => {
    if (acceptedFiles.length === 0) return
    
    setUploading(true)
    setError(null)
    
    const formData = new FormData()
    formData.append('file', acceptedFiles[0])
    
    try {
      const response = await axios.post(`${API_BASE}/files/upload`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      })
      onFileUploaded(response.data)
    } catch (err) {
      setError(err.response?.data?.detail || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }, [onFileUploaded])

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/json': ['.json'],
      'text/plain': ['.txt'],
      'application/pdf': ['.pdf']
    },
    maxFiles: 1
  })

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Upload Dataset</h2>
      <div
        {...getRootProps()}
        className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors
          ${isDragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'}`}
      >
        <input {...getInputProps()} />
        {uploading ? (
          <div className="flex items-center justify-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
            <span className="ml-2">Uploading...</span>
          </div>
        ) : isDragActive ? (
          <p className="text-blue-500">Drop the file here...</p>
        ) : (
          <div>
            <p className="text-gray-600">Drag & drop a file here, or click to select</p>
            <p className="text-sm text-gray-400 mt-2">Supported: CSV, JSON, TXT, PDF (up to 100MB)</p>
          </div>
        )}
      </div>
      {error && <p className="text-red-500 mt-2">{error}</p>}
    </div>
  )
}

function TaskSelector({ selectedTasks, onTasksChange }) {
  const tasks = [
    { id: 'descriptive_stats', name: 'Descriptive Statistics', desc: 'Rows, columns, null %, unique counts, min/max/mean' },
    { id: 'linear_regression', name: 'Linear Regression', desc: 'R2, RMSE, MAE metrics' },
    { id: 'logistic_regression', name: 'Logistic Regression', desc: 'Accuracy, precision, recall, F1' },
    { id: 'kmeans', name: 'K-Means Clustering', desc: 'Inertia and silhouette metrics' },
    { id: 'fpgrowth', name: 'FP-Growth', desc: 'Frequent patterns and association rules' },
    { id: 'time_series', name: 'Time Series Analysis', desc: 'Temporal patterns' }
  ]

  const toggleTask = (taskId) => {
    if (selectedTasks.includes(taskId)) {
      onTasksChange(selectedTasks.filter(t => t !== taskId))
    } else {
      onTasksChange([...selectedTasks, taskId])
    }
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Select Processing Tasks</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {tasks.map(task => (
          <div
            key={task.id}
            onClick={() => toggleTask(task.id)}
            className={`p-4 rounded-lg border-2 cursor-pointer transition-all
              ${selectedTasks.includes(task.id) 
                ? 'border-blue-500 bg-blue-50' 
                : 'border-gray-200 hover:border-gray-300'}`}
          >
            <div className="flex items-center">
              <input
                type="checkbox"
                checked={selectedTasks.includes(task.id)}
                onChange={() => {}}
                className="h-4 w-4 text-blue-500"
              />
              <span className="ml-2 font-medium">{task.name}</span>
            </div>
            <p className="text-sm text-gray-500 mt-1">{task.desc}</p>
          </div>
        ))}
      </div>
    </div>
  )
}

function WorkerConfig({ workerCounts, onWorkerCountsChange }) {
  const options = [1, 2, 4, 8]

  const toggleWorker = (count) => {
    if (workerCounts.includes(count)) {
      onWorkerCountsChange(workerCounts.filter(w => w !== count))
    } else {
      onWorkerCountsChange([...workerCounts, count].sort((a, b) => a - b))
    }
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Worker Configuration</h2>
      <p className="text-gray-600 mb-4">Select cluster sizes to benchmark performance:</p>
      <div className="flex gap-4">
        {options.map(count => (
          <button
            key={count}
            onClick={() => toggleWorker(count)}
            className={`px-6 py-3 rounded-lg font-medium transition-all
              ${workerCounts.includes(count)
                ? 'bg-blue-500 text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}
          >
            {count} Worker{count > 1 ? 's' : ''}
          </button>
        ))}
      </div>
    </div>
  )
}

function JobMonitor({ job, metrics }) {
  if (!job) return null

  const statusColors = {
    pending: 'bg-yellow-100 text-yellow-800',
    running: 'bg-blue-100 text-blue-800',
    completed: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800'
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-semibold">Job Status</h2>
        <span className={`px-3 py-1 rounded-full text-sm font-medium ${statusColors[job.status]}`}>
          {job.status.toUpperCase()}
        </span>
      </div>
      
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div>
          <p className="text-sm text-gray-500">File</p>
          <p className="font-medium">{job.filename}</p>
        </div>
        <div>
          <p className="text-sm text-gray-500">Job ID</p>
          <p className="font-mono text-sm">{job.job_id.slice(0, 8)}...</p>
        </div>
      </div>

      <div className="mb-4">
        <p className="text-sm text-gray-500 mb-2">Logs</p>
        <div className="bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-sm max-h-48 overflow-y-auto">
          {job.logs.length > 0 ? (
            job.logs.map((log, i) => <div key={i}>{log}</div>)
          ) : (
            <div>Waiting for logs...</div>
          )}
        </div>
      </div>

      {job.results && job.results.length > 0 && (
        <div className="mb-4">
          <p className="text-sm text-gray-500 mb-2">Results ({job.results.length})</p>
          <div className="space-y-2">
            {job.results.map((result, i) => (
              <div key={i} className="flex items-center justify-between p-3 bg-gray-50 rounded">
                <div>
                  <span className="font-medium">{result.task_type}</span>
                  <span className="text-gray-500 ml-2">({result.worker_count} workers)</span>
                </div>
                <div className="flex items-center gap-4">
                  <span className="text-sm">{result.execution_time_seconds.toFixed(2)}s</span>
                  <span className={`px-2 py-1 rounded text-xs ${
                    result.status === 'completed' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                  }`}>
                    {result.status}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function PerformanceCharts({ metrics }) {
  if (!metrics || metrics.length === 0) return null

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Performance Analysis</h2>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div>
          <h3 className="font-medium mb-2">Execution Time by Worker Count</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={metrics.flatMap(m => 
              m.worker_counts.map((w, i) => ({
                task: m.task_type,
                workers: `${w}w`,
                time: m.execution_times[i]
              }))
            )}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="workers" />
              <YAxis label={{ value: 'Time (s)', angle: -90, position: 'insideLeft' }} />
              <Tooltip />
              <Legend />
              <Bar dataKey="time" fill="#3B82F6" name="Execution Time" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div>
          <h3 className="font-medium mb-2">Speedup Factor</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={metrics[0]?.worker_counts.map((w, i) => ({
              workers: w,
              ...Object.fromEntries(metrics.map(m => [m.task_type, m.speedups[i]]))
            })) || []}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="workers" label={{ value: 'Workers', position: 'bottom' }} />
              <YAxis label={{ value: 'Speedup', angle: -90, position: 'insideLeft' }} />
              <Tooltip />
              <Legend />
              {metrics.map((m, i) => (
                <Line 
                  key={m.task_type}
                  type="monotone" 
                  dataKey={m.task_type} 
                  stroke={['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899'][i % 6]}
                  strokeWidth={2}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="mt-6">
        <h3 className="font-medium mb-2">Performance Summary</h3>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Task</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Workers</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Time (s)</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Speedup</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Efficiency</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {metrics.flatMap(m => 
                m.worker_counts.map((w, i) => (
                  <tr key={`${m.task_type}-${w}`}>
                    <td className="px-4 py-2 text-sm">{m.task_type}</td>
                    <td className="px-4 py-2 text-sm">{w}</td>
                    <td className="px-4 py-2 text-sm">{m.execution_times[i].toFixed(3)}</td>
                    <td className="px-4 py-2 text-sm">{m.speedups[i].toFixed(2)}x</td>
                    <td className="px-4 py-2 text-sm">{(m.efficiencies[i] * 100).toFixed(1)}%</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function ResultsViewer({ job }) {
  const [selectedResult, setSelectedResult] = useState(null)

  if (!job || !job.results || job.results.length === 0) return null

  const completedResults = job.results.filter(r => r.status === 'completed')

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Detailed Results</h2>
      
      <div className="flex gap-2 mb-4 flex-wrap">
        {completedResults.map((result, i) => (
          <button
            key={i}
            onClick={() => setSelectedResult(result)}
            className={`px-3 py-1 rounded text-sm transition-colors
              ${selectedResult === result 
                ? 'bg-blue-500 text-white' 
                : 'bg-gray-100 hover:bg-gray-200'}`}
          >
            {result.task_type} ({result.worker_count}w)
          </button>
        ))}
      </div>

      {selectedResult && (
        <div className="bg-gray-50 p-4 rounded-lg">
          <h3 className="font-medium mb-2">{selectedResult.task_type} Results</h3>
          <pre className="text-sm overflow-x-auto">
            {JSON.stringify(selectedResult.metrics, null, 2)}
          </pre>
          {selectedResult.output_path && (
            <a
              href={`/api/files/download/${selectedResult.output_path}`}
              className="inline-block mt-3 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            >
              Download Full Results
            </a>
          )}
        </div>
      )}
    </div>
  )
}

export default function App() {
  const [uploadedFile, setUploadedFile] = useState(null)
  const [selectedTasks, setSelectedTasks] = useState(['descriptive_stats'])
  const [workerCounts, setWorkerCounts] = useState([1, 2, 4, 8])
  const [currentJob, setCurrentJob] = useState(null)
  const [metrics, setMetrics] = useState(null)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [activeTab, setActiveTab] = useState('upload')

  useEffect(() => {
    let interval
    if (currentJob && (currentJob.status === 'pending' || currentJob.status === 'running')) {
      interval = setInterval(async () => {
        try {
          const response = await axios.get(`${API_BASE}/jobs/${currentJob.job_id}`)
          setCurrentJob(response.data)
          
          if (response.data.status === 'completed') {
            const metricsResponse = await axios.get(`${API_BASE}/jobs/${currentJob.job_id}/metrics`)
            setMetrics(metricsResponse.data)
          }
        } catch (err) {
          console.error('Failed to fetch job status', err)
        }
      }, 2000)
    }
    return () => clearInterval(interval)
  }, [currentJob])

  const handleSubmitJob = async () => {
    if (!uploadedFile || selectedTasks.length === 0) return
    
    setIsSubmitting(true)
    try {
      const response = await axios.post(`${API_BASE}/jobs`, {
        file_id: uploadedFile.file_id,
        tasks: selectedTasks,
        worker_counts: workerCounts
      })
      setCurrentJob(response.data)
      setActiveTab('monitor')
    } catch (err) {
      alert(err.response?.data?.detail || 'Failed to submit job')
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <div className="min-h-screen bg-gray-100">
      <header className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <h1 className="text-3xl font-bold text-gray-900">Spark Data Processor</h1>
          <p className="text-gray-600">Cloud-based distributed data processing with ML capabilities</p>
        </div>
      </header>

      <nav className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4">
          <div className="flex gap-4">
            {['upload', 'monitor', 'results'].map(tab => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`px-4 py-3 font-medium border-b-2 transition-colors
                  ${activeTab === tab 
                    ? 'border-blue-500 text-blue-600' 
                    : 'border-transparent text-gray-500 hover:text-gray-700'}`}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </div>
        </div>
      </nav>

      <main className="max-w-7xl mx-auto px-4 py-8">
        {activeTab === 'upload' && (
          <div className="space-y-6">
            <FileUpload onFileUploaded={(file) => {
              setUploadedFile(file)
            }} />
            
            {uploadedFile && (
              <>
                <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                  <p className="text-green-800">
                    <span className="font-medium">File uploaded:</span> {uploadedFile.filename} 
                    ({(uploadedFile.file_size / 1024).toFixed(1)} KB)
                  </p>
                </div>
                
                <TaskSelector 
                  selectedTasks={selectedTasks} 
                  onTasksChange={setSelectedTasks} 
                />
                
                <WorkerConfig 
                  workerCounts={workerCounts} 
                  onWorkerCountsChange={setWorkerCounts} 
                />
                
                <div className="flex justify-end">
                  <button
                    onClick={handleSubmitJob}
                    disabled={isSubmitting || selectedTasks.length === 0}
                    className="px-6 py-3 bg-blue-600 text-white rounded-lg font-medium
                      hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed
                      flex items-center gap-2"
                  >
                    {isSubmitting ? (
                      <>
                        <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                        Submitting...
                      </>
                    ) : (
                      'Start Processing Job'
                    )}
                  </button>
                </div>
              </>
            )}
          </div>
        )}

        {activeTab === 'monitor' && (
          <div className="space-y-6">
            <JobMonitor job={currentJob} metrics={metrics} />
            {metrics && <PerformanceCharts metrics={metrics} />}
          </div>
        )}

        {activeTab === 'results' && (
          <div className="space-y-6">
            <ResultsViewer job={currentJob} />
            {metrics && <PerformanceCharts metrics={metrics} />}
          </div>
        )}
      </main>

      <footer className="bg-white border-t mt-8">
        <div className="max-w-7xl mx-auto px-4 py-6 text-center text-gray-500">
          <p>Spark Data Processor - Distributed ML Processing Platform</p>
        </div>
      </footer>
    </div>
  )
}
