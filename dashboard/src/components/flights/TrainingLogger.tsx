import { useEffect, useState } from 'react'

import { Pod } from '../../models/pod'
import { Flight } from '../../models/flight'

export interface ITrainingLoggerProps {
  pod: Pod
  flight: Flight
  loggerId: string
}

interface ITrainingLogger {
  name: string
  color: string
}

const loggers = new Map<string, ITrainingLogger>([
  [
    'tensorboard',
    {
      name: 'TensorBoard',
      color: 'yellow-400',
    },
  ],
])

const TrainingLogger: React.FunctionComponent<ITrainingLoggerProps> = (props) => {
  const { pod, flight, loggerId: id } = props
  const [logger, setLogger] = useState<ITrainingLogger>()
  const [isOpening, setIsOpening] = useState(false)

  let openedWindow: Window | null = null

  useEffect(() => {
    const l = loggers.get(id)
    if (l) {
      setLogger(l)
    }
  }, [id])

  const onClick = async () => {
    const options = {
      method: 'POST',
    }

    setIsOpening(true)

    const url = `/api/v0.1/pods/${pod.name}/training_runs/${flight.id}/loggers/${id}`
    const resp = await fetch(url, options)
    if (!resp.ok) {
      setIsOpening(false)
      console.error(resp.status, resp.statusText, await resp.text())
      return
    }

    const address = await resp.text()
    if (address) {
      if (openedWindow) {
        if (openedWindow.location.href != address) {
          openedWindow.location.href = address
        } else {
          openedWindow.location.reload()
        }
      } else {
        openedWindow = window.open(address, '_blank')
      }
    }
    setIsOpening(false)
  }

  return (
    <button onClick={onClick} className={`bg-${logger?.color} text-xs rounded py-1 px-2`}>
      {(isOpening ? 'Opening ' : 'Open ') + logger?.name}
    </button>
  )
}

export default TrainingLogger
