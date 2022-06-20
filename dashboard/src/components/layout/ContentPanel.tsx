import React from 'react'

interface ContentPanelProps {
  children?: React.ReactNode
}

const ContentPanel: React.FC<ContentPanelProps> = (props) => {
  const { children } = props
  return (
    <div className="flex flex-col flex-grow bg-gradient-to-b from-white to-cloud p-4">
      <div>{children}</div>
      <div className="flex-grow" />
    </div>
  )
}

export default ContentPanel
