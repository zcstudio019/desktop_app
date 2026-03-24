import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'

// eslint-disable-next-line @typescript-eslint/no-non-null-assertion -- Standard React entry point: root element is guaranteed to exist in index.html
ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
