import React from 'react';
import { useLocation } from 'react-router-dom';

import PodHeader from '../components/app/PodHeader';
import Card from '../components/layout/Card';
import { usePod } from '../models/pod';
import { useFlights } from '../models/flight';
import FlightChart from '../components/flights/FlightChart';

interface PodProps {
  podName: string
}

const PodPage: React.FunctionComponent<PodProps> = () => {
  const location = useLocation()
  const podNamePathIndex = location.pathname.lastIndexOf('/') + 1
  const podName = location.pathname.substring(podNamePathIndex)

  const { data: pod, error: podError } = usePod(podName);
  const { data: flights, error: flightsError } = useFlights(podName)

  return (
    <div className="flex flex-col flex-grow">
      { !podError && pod &&
        <div className="mb-2">
          <PodHeader pod={pod}></PodHeader>
          <h2 className="ml-2 mb-2 font-spice tracking-spice text-s uppercase">Flights</h2>
          <div className="p-2">
            { !flightsError && flights.map((flight, i) => (
                <div key={i}>
                  <Card>
                    <FlightChart flight={flight} />
                  </Card>
                </div>
            ))}
            { (!flights || flights.length === 0) &&
              <span>Pod has no flights</span>
            }
          </div>
        </div>
      }
    </div>
  );
};

export default PodPage;
