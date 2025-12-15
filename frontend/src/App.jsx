import React, { useState, useEffect, useCallback } from 'react'
import { useDropzone } from 'react-dropzone'
import axios from 'axios'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
  BarChart, Bar
} from 'recharts'

const API_BASE = `${import.meta.env.VITE_API_URL}/api`

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
      const response = await axios.post(
        `${API_BASE}/files/upload`,
        formData,
        { headers: { 'Content-Type': 'multipart/form-data' } }
      )
      onFileUploaded(response.data)
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
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Upload Dataset</h2>
      <div
        {...getRootProps()}
        className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer
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
function TaskSelector({ selectedTasks, onTasksChange }) {
  const tasks = [
    'descriptive_stats',
    'linear_regression',
    'logistic_regression',
    'kmeans',
    'fpgrowth',
    'time_series'
  ]

  const toggleTask = (task) => {
    onTasksChange(
      selectedTasks.includes(task)
        ? selectedTasks.filter(t => t !== task)
        : [...selectedTasks, task]
    )
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Tasks</h2>
      <div className="flex flex-wrap gap-2">
        {tasks.map(task => (
          <button
            key={task}
            onClick={() => toggleTask(task)}
            className={`px-4 py-2 rounded
              ${selectedTasks.includes(task)
                ? 'bg-blue-500 text-white'
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
  const [uploadedFile, setUploadedFile] = useState(null)
  const [selectedTasks, setSelectedTasks] = useState(['descriptive_stats'])
  const [job, setJob] = useState(null)
  const [metrics, setMetrics] = useState(null)

  /* Poll job status */
  useEffect(() => {
    if (!job || job.status === 'completed') return

    const interval = setInterval(async () => {
      try {
        const res = await axios.get(`${API_BASE}/jobs/${job.job_id}`)
        setJob(res.data)

        if (res.data.status === 'completed') {
          const m = await axios.get(`${API_BASE}/jobs/${job.job_id}/metrics`)
          setMetrics(m.data)
        }
      } catch (e) {
        console.error(e)
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [job])

  const startJob = async () => {
    const res = await axios.post(`${API_BASE}/jobs`, {
      file_id: uploadedFile.file_id,
      tasks: selectedTasks,
      worker_counts: [1, 2, 4, 8]
    })
    setJob(res.data)
  }

  return (
    <div className="min-h-screen bg-gray-100">
      <header className="bg-white shadow p-6">
        <h1 className="text-3xl font-bold">Spark Data Processor</h1>
        <p className="text-gray-600">Distributed ML Processing</p>
      </header>

      <main className="max-w-6xl mx-auto p-6 space-y-6">
        <FileUpload onFileUploaded={setUploadedFile} />

        {uploadedFile && (
          <>
            <TaskSelector
              selectedTasks={selectedTasks}
              onTasksChange={setSelectedTasks}
            />

            <button
              onClick={startJob}
              className="px-6 py-3 bg-blue-600 text-white rounded"
            >
              Start Job
            </button>
          </>
        )}

        {job && (
          <div className="bg-white p-6 rounded shadow">
            <p>Status: <b>{job.status}</b></p>
          </div>
        )}

        {metrics && (
          <div className="bg-white p-6 rounded shadow">
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={metrics[0].execution_times.map((t, i) => ({
                workers: metrics[0].worker_counts[i],
                time: t
              }))}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="workers" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="time" fill="#3B82F6" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </main>
    </div>
  )
                                                             }
