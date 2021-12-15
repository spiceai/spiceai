import React, { useEffect } from 'react'
import { Pod } from '../../models/pod'
import { Flight } from '../../models/flight'
import ErrorAlert from '../../components/alerts/Error'

interface PodHeaderProps {
  pod: Pod
  flights: Flight[]
}

const PodHeader: React.FunctionComponent<PodHeaderProps> = (props) => {
  const { pod, flights } = props

  const [trainButtonText, setTrainButtonText] = React.useState('Loading')
  const [errorMessage, setErrorMessage] = React.useState('')
  const [trainingRequested, setTrainingRequested] = React.useState(false)

  const onStartTraining = async () => {
    setTrainingRequested(true)
    const url = `/api/v0.1/pods/${pod.name}/train`
    const options = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        episodes: pod.episodes,
      }),
    }
    const resp = await fetch(url, options)
    if (!resp.ok) {
      setTrainingRequested(false)
      setErrorMessage(`failed to start training - ${await resp.text()}`)
    }
  }

  useEffect(() => {
    if (flights.length) {
      const flight = flights[0]
      if (!flight.end || flight.end.getTime() < flight.start.getTime()) {
        setTrainingRequested(false)
        setTrainButtonText('Training...')
        return
      } else if (trainingRequested) {
        setTrainButtonText('Starting...')
        return
      }
    }
    if (!trainingRequested) {
      setTrainButtonText('Start Training')
    }
  }, [flights, trainingRequested])

  return (
    <React.Fragment>
      {errorMessage && (
        <div className="sticky">
          <ErrorAlert message={errorMessage} onClose={() => setErrorMessage('')} />
        </div>
      )}
      <div className="m-4 flex gap-4">
        <div>
          <div className="bg-red-500 rounded-full h-16 w-16 flex items-center justify-center text-2xl text-semibold uppercase">
            {props.pod.name.substr(0, 1)}
          </div>
        </div>
        <div>
          <h1 className="text-2xl text-blue-600 text-semibold">{props.pod.name}</h1>
          <span className="text-sm text-gray-500">{props.pod.manifest_path}</span>
        </div>
        <div className="flex-grow"></div>
        <div className="flex items-center">
          <button
            onClick={onStartTraining}
            disabled={trainButtonText !== 'Start Training'}
            className="w-32 bg-blue-500 hover:bg-blue-700 text-sm text-white font-semibold py-2 px-3 rounded disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {trainButtonText}
          </button>
        </div>
      </div>
    </React.Fragment>
  )
}

export default PodHeader
