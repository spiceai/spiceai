import { marked } from 'marked'
import React, { useState, useEffect } from 'react'
import acknowledgements from '../content/acknowledgements.md'

const Acknowledgements: React.FunctionComponent = () => {
  const [acknowledgementsMarkdown, setAcknowledgementsMarkdown] = useState('')

  useEffect(() => {
    const fetchContent = async () => {
      const response = await fetch(acknowledgements)
      const text = await response.text()
      setAcknowledgementsMarkdown(text)
    }
    fetchContent()
  }, [])

  return (
    <section className="p-8">
      <div
        className="markdown"
        dangerouslySetInnerHTML={{ __html: marked(acknowledgementsMarkdown) }}
      ></div>
    </section>
  )
}

export default Acknowledgements
