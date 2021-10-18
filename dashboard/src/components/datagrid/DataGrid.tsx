import React from 'react';

import { useObservations } from '../../models/observation';
import { Pod } from '../../models/pod';

interface DataGridProps {
  pod: Pod;
}

const DataGrid: React.FunctionComponent<DataGridProps> = (props) => {
  const {
    data: observationData,
    error: observationError,
    isLoading: observationsLoading,
  } = useObservations(props.pod.name);

  if (observationError) {
    return (
      <div className="p-4">Error loading observations: {JSON.stringify(observationError)}</div>
    );
  }

  if (observationsLoading) {
    return <div className="p-4">Loading observations...</div>;
  }

  return (
    <div className="bg-white rounded rounded-b-none border border-gray-300">
      {observationData.length === 0 ? (
        <div className="p-4">No data</div>
      ) : (
        <table className="text-sm table-auto">
          <thead className="bg-gradient-to-r from-sidebar-gradient-start to-sidebar-gradient-end p-1 uppercase text-white font-heading">
            <tr>
              <th>time</th>
              {observationData[0].measurements &&
                Object.keys(observationData[0].measurements).map((h, i) => {
                  return <th key={i}>{h}</th>;
                })}
              {observationData[0].categories &&
                Object.keys(observationData[0].categories).map((h, i) => {
                  return <th key={i}>{h}</th>;
                })}
              <th>tags</th>
            </tr>
          </thead>
          <tbody className="block h-120 overflow-scroll">
            {observationData.map((o, i) => {
              return (
                <tr key={i}>
                  <td className="border-r">{new Date(o.time * 1000).toLocaleString()}</td>
                  {observationData[0].measurements &&
                    Object.values(observationData[0].measurements).map((h, i) => {
                      return (
                        <td key={i} className="px-1">
                          {h}
                        </td>
                      );
                    })}
                  {observationData[0].categories &&
                    Object.values(observationData[0].categories).map((h, i) => {
                      return (
                        <td key={i} className="px-1">
                          {h}
                        </td>
                      );
                    })}
                  {observationData[0].tags && <td>{observationData[0].tags.join(' ')}</td>}
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
};

export default DataGrid;
