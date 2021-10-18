import React from 'react';

import { useObservations } from '../../models/observation';
import { Pod } from '../../models/pod';
import GridItem from './GridItem';

interface DataGridProps {
  pod: Pod;
}

const DataGrid: React.FunctionComponent<DataGridProps> = (props) => {
  const {
    data: observationData,
    isLoading: observationsLoading,
    error: observationError,
  } = useObservations(props.pod.name);

  if (observationError) {
    return (
      <div className="p-2">Error loading observation data: {JSON.stringify(observationError)}</div>
    );
  }

  return (
    <div className="h-120 bg-white rounded rounded-b-none border border-b-0 border-gray-300 overflow-hidden">
      <div className="flex bg-gradient-to-r from-sidebar-gradient-start to-sidebar-gradient-end p-1">
        <span className="uppercase text-white font-heading text-sm justify-items-center">
          Coinbase: BTCUSD
        </span>
      </div>

      {observationData.length === 0 ? (
        <div>No data</div>
      ) : (
        <div className="grid grid-cols-7 justify-items-center overflow-scroll"></div>
      )}
    </div>
  );
};

export default DataGrid;
