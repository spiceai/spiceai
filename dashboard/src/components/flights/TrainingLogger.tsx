import { useEffect, useState } from 'react'

export interface ITrainingLoggerProps {
  id: string
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
  const { id } = props

  const [logger, setLogger] = useState<ITrainingLogger>()

  useEffect(() => {
    const l = loggers.get(id)
    if (l) {
      setLogger(l)
    }
  }, [id])

  return <button className={`bg-${logger?.color} text-xs rounded py-1 px-2`}>{logger?.name}</button>
}

export default TrainingLogger
