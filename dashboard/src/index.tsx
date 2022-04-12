import React from 'react'
import ReactDOM from 'react-dom'
import './tailwind.output.css'
import App from './App'
import reportWebVitals from './reportWebVitals'

const rootElement = document.getElementById('root')
if (!rootElement) throw new Error('Failed to find the root element')
const root = (ReactDOM as any).createRoot(rootElement)

root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals()
