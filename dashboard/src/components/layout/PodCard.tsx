import { useHistory } from 'react-router-dom';
import { Sparklines, SparklinesLine } from 'react-sparklines';
import { useFlights } from '../../models/flight'

interface PodCardProps {
  isLoading: boolean;
  podName: string;
  fqid: string;
  route: string;
}

const PodCard: React.FunctionComponent<PodCardProps> = (props) => {
  const { isLoading: podIsLoading, podName, route, fqid } = props;
  const history = useHistory();

  const { data: flights, isLoading: flightsLoading } = useFlights(props.podName)
  const hasFlightData = !flightsLoading && flights && flights.length > 0

  const sparkData = hasFlightData
    ? flights.map(flight => {
        return flight.episodes.length > 0
          ? flight.episodes.map(ep => ep.reward).reduce((total, reward) => {
              return total + reward
            }, 0) / flight.episodes.length
          : 0
      })
    : [5, 10, 6, 20, 15, 10, 20, 25, 25]

  function handleClick() {
    history.push(route);
  }

  if (podIsLoading) {
    <div className="border p-10 rounded shadow bg-white">
      <div className="animate-pulse flex items-center justify-items-center gap-4">
        <div className="h-12 w-12 bg-primary rounded-full"></div>
        <div className="flex flex-col gap-2 items-start">
          <div className="h-4 w-48 bg-blue-200 rounded"></div>
          <div className="h-4 w-36 bg-gray-400 rounded"></div>
        </div>
      </div>
    </div>;
  }

  return (
    <div className="flex flex-row border p-6 h-32 rounded shadow bg-white cursor-pointer" onClick={handleClick}>
      <div className="flex items-center justify-items-center gap-4">
        <div className="flex flex-col items-start">
          <p className="text-xl text-link font-semibold">{podName}</p>
          <span className="text-black">{fqid}</span>
        </div>
      </div>
      <div className="flex-grow"></div>
      <div className="w-60">
        <span className="text-xs">Average flight rewards (last 5)</span>
        <Sparklines data={sparkData} min={0} limit={5}>
          <SparklinesLine color={hasFlightData ? 'blue' : undefined} />
        </Sparklines>
      </div>
    </div>
  );
};

export default PodCard;
