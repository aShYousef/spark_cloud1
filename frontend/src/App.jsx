import React, { useState, useEffect, useCallback } from 'react'
import { useDropzone } from 'react-dropzone'
import axios from 'axios'
import {
  BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer
} from 'recharts'

/* =========================
   API BASE (Render Backend)
========================= */
const API_BASE = "https://cloud1.onrender.com/api"

/* =========================
   File Upload
========================= */
function FileUpload({ onFileUploaded }) {
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState(null)

  const onDrop = useCallback(async (acceptedFiles) => {
    if (!acceptedFiles.length) return

    setUploading(true)
    setError(null)

    const formData = new FormData()
    formData.append('file', acceptedFiles[0])

    try {
      const res = await axios.post(
        `${API_BASE}/files/upload`,
        formData,
        { headers: { 'Content-Type': 'multipart/form-data' } }
      )
      onFileUploaded(res.data)
    } catch (err) {
      setError(err.response?.data?.detail || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }, [onFileUploaded])

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    maxFiles: 1
  })

  return (
    <div className="bg-white p-6 rounded shadow">
      <h2 className="text-xl font-semibold mb-4">Upload Dataset</h2>
      <div
        {...getRootProps()}
        className={`border-2 border-dashed rounded p-8 text-center cursor-pointer
          ${isDragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300'}`}
      >
        <input {...getInputProps()} />
        {uploading ? 'Uploading...' : 'Drag & drop or click to upload'}
      </div>
      {error && <p className="text-red-500 mt-2">{error}</p>}
    </div>
  )
}

/* =========================
   Task Selector
========================= */
function TaskSelector({ selectedTasks, onChange }) {
  const tasks = [
    'descriptive_stats',
    'linear_regression',
    'logistic_regression',
    'kmeans',
    'fpgrowth',
    'time_series'
  ]

  const toggle = (task) => {
    onChange(
      selectedTasks.includes(task)
        ? selectedTasks.filter(t => t !== task)
        : [...selectedTasks, task]
    )
  }

  return (
    <div className="bg-white p-6 rounded shadow">
      <h2 className="text-xl font-semibold mb-4">Tasks</h2>
      <div className="flex flex-wrap gap-2">
        {tasks.map(task => (
          <button
            key={task}
            onClick={() => toggle(task)}
            className={`px-4 py-2 rounded
              ${selectedTasks.includes(task)
                ? 'bg-blue-600 text-white'
                : 'bg-gray-200'}`}
          >
            {task}
          </button>
        ))}
      </div>
    </div>
  )
}

/* =========================
   App
========================= */
export default function App() {
  const [file, setFile] = useState(null)
  const [tasks, setTasks] = useState(['descriptive_stats'])
  const [job, setJob] = useState(null)
  const [metrics, setMetrics] = useState(null)

  useEffect(() => {
    if (!job || ['completed', 'failed', 'cancelled'].includes(job.status)) return

    const interval = setInterval(async () => {
      const res = await axios.get(`${API_BASE}/jobs/${job.job_id}`)
      setJob(res.data)

      if (res.data.status === 'completed') {
        const m = await axios.get(`${API_BASE}/jobs/${job.job_id}/metrics`)
        setMetrics(m.data)
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [job])

  const startJob = async () => {
    const res = await axios.post(`${API_BASE}/jobs`, {
      file_id: file.file_id,
      tasks,
      worker_counts: [1, 2, 4, 8]
    })
    setJob(res.data)
  }

  return (
    <div className="min-h-screen bg-gray-100">
      <header className="bg-white p-6 shadow">
        <h1 className="text-3xl font-bold">Spark Data Processor</h1>
        <p className="text-gray-600">Distributed & Parallel ML Analytics</p>
      </header>

      <main className="max-w-6xl mx-auto p-6 space-y-6">
        <FileUpload onFileUploaded={setFile} />

        {file && (
          <>
            <TaskSelector selectedTasks={tasks} onChange={setTasks} />
            <button
              onClick={startJob}
              className="px-6 py-3 bg-blue-600 text-white rounded"
            >
              Start Job
            </button>
          </>
        )}

        {job && (
          <div className="bg-white p-4 rounded shadow">
            <b>Status:</b> {job.status}
          </div>
        )}

        {metrics && metrics.length > 0 && (
          <div className="bg-white p-6 rounded shadow">
            <h3 className="font-semibold mb-2">Execution Time vs Workers</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart
                data={metrics[0].worker_counts.map((w, i) => ({
                  workers: w,
                  time: metrics[0].execution_times[i]
                }))}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="workers" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="time" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </main>
    </div>
  )
}
