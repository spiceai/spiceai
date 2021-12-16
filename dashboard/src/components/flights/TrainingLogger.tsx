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

    const url = `/api/v0.1/pods/${pod.name}/training_runs/${flight.id}/loggers/${id}`
    const resp = await fetch(url, options)
    if (!resp.ok) {
      console.error(await resp.text())
    }
  }

  return (
    <button onClick={onClick} className={`bg-${logger?.color} text-xs rounded py-1 px-2`}>
      {logger?.name}
    </button>
  )
}

export default TrainingLogger
