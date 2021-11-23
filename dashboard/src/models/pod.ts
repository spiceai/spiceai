import { useState, useEffect } from 'react'
import useSWR from 'swr'

import { getApiPath } from '../util/api'

export interface Pod {
  name: string
  manifest_path: string
  episodes: number
  identifiers: string[]
  measurements: string[]
  categories: string[]
}

export interface PodsResponse {
  data: Pod[]
  isLoading: boolean
  error: any
}

export interface PodResponse {
  data: Pod | undefined
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

export function usePods(): PodsResponse {
  const path = getApiPath('/pods')
  const { data, error } = useSWR(path, fetcher, {
    refreshInterval: 250,
  })
  const [pods, setPods] = useState<Pod[]>([])

  useEffect(() => {
    if (data) {
      setPods(data)
    }
  }, [data])

  pods.sort((a, b) => {
    return a.name.localeCompare(b.name)
  })

  return {
    data: pods,
    isLoading: !error && !data,
    error,
  }
}

export function usePod(podName: string): PodResponse {
  const path = getApiPath(`/pods/${podName}`)
  const { data, error } = useSWR(path, fetcher, {
    refreshInterval: 250,
  })
  const [pod, setPod] = useState<Pod>()

  useEffect(() => {
    if (data) {
      setPod(data)
    }
  }, [data])

  return {
    data: pod,
    isLoading: !error && !data,
    error,
  }
}
