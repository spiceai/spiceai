import { useState, useEffect } from 'react'
import useSWR from 'swr'

import { getApiPath } from '../util/api'
import { Episode, newEpisode } from './episode'

export interface Flight {
  id: string
  algorithm: string
  start: Date
  end: Date
  episodes: Episode[]
  loggers: string[]
}

export interface FlightsResponse {
  data: Flight[]
  isLoading: boolean
  error: any
}

const requestInit = {
  headers: new Headers({
    Accept: 'application/json',
  }),
}

const fetcher = async (url: string): Promise<any> => {
  const response = await fetch(url, requestInit)
  return response.json()
}

export function useFlights(podName: string): FlightsResponse {
  const path = getApiPath(`/pods/${podName}/training_runs`)
  const { data, error } = useSWR(path, fetcher, {
    refreshInterval: 200,
  })
  const [flights, setFlights] = useState<Flight[]>([])

  useEffect(() => {
    if (data) {
      setFlights(
        (
          data.map((flight: any) => {
            return {
              id: flight.id,
              algorithm: flight.algorithm,
              start: new Date(flight.start * 1000),
              end: flight.end ? new Date(flight.end * 1000) : null,
              episodes: flight.episodes ? flight.episodes.map((ep: any) => newEpisode(ep)) : [],
              loggers: flight.loggers,
            } as Flight
          }) as Flight[]
        ).sort((a, b) => {
          return b.start.getTime() - a.start.getTime()
        })
      )
    }
  }, [data])

  return {
    data: flights,
    isLoading: !error && !data,
    error,
  }
}
