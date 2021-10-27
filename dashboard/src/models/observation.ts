import { useState, useEffect } from 'react';
import useSWR from 'swr';

import { getApiPath } from '../util/api';

export interface Observation {
  time: number;
  measurements: { [key: string]: number };
  categories: { [key: string]: string };
  tags: string[];
}

export interface ObservationsResponse {
  data: Observation[];
  isLoading: boolean;
  error: any;
}

const requestInit = {
  headers: new Headers({
    Accept: 'application/json',
  }),
};

const fetcher = async (url: string): Promise<any> => {
  const response = await fetch(url, requestInit);
  return response.json();
};

export function useObservations(podName: string): ObservationsResponse {
  const path = getApiPath(`/pods/${podName}/observations`);
  const { data, error } = useSWR(path, fetcher, {
    refreshInterval: 100,
  });
  const [observations, setObservations] = useState<Observation[]>([]);

  useEffect(() => {
    if (data) {
      setObservations(data);
    }
  }, [data]);

  return {
    data: observations,
    isLoading: !error && !data,
    error,
  };
}
