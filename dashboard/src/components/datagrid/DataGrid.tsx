import React from 'react';

import { useObservations } from '../../models/observation';
import { Pod } from '../../models/pod';
import GridItem from './GridItem';

interface DataGridProps {
  pod: Pod;
}

const DataGrid: React.FunctionComponent<DataGridProps> = (props) => {
  const { data: observationData, error: observationError } = useObservations(props.pod.name);

  // TODO: Handle isLoading and error states
  if (!observationData || observationError) {
    return <>Error loading observation data: {observationError}</>;
  }

  return (
    <div className="h-120 bg-white rounded rounded-b-none border border-b-0 border-gray-300 overflow-hidden">
      <div className="flex bg-gradient-to-r from-sidebar-gradient-start to-sidebar-gradient-end p-1">
        <span className="uppercase text-white font-heading text-sm justify-items-center">
          Coinbase: BTCUSD
        </span>
      </div>

      <div className="grid grid-cols-7 justify-items-center overflow-scroll">
        <GridItem heading>Time</GridItem>
        {observationData.map((od, i) => (
          <div key={i}>
            <GridItem>{new Date(od.time).toLocaleString()}</GridItem>
          </div>
        ))}
      </div>
    </div>
  );
};

export default DataGrid;
