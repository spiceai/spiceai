import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';

import PodHeader from '../components/app/PodHeader';
import Card from '../components/layout/Card';
import { usePod } from '../models/pod';
import { useFlights } from '../models/flight';
import { useObservations } from '../models/observation';
import FlightChart from '../components/flights/FlightChart';
import DataEditor, {
  DataEditorContainer,
  GridColumn,
  GridCell,
  GridCellKind,
} from '@glideapps/glide-data-grid';

interface PodProps {
  podName: string;
}

interface GridProps {
  columns: GridColumn[];
  gridDataFunc: ([col, row]: readonly [number, number]) => GridCell;
}

// TODO: Resize dynamically
const gridWidth = 900;
const gridHeight = 600;

const PodPage: React.FunctionComponent<PodProps> = () => {
  const location = useLocation();
  const podNamePathIndex = location.pathname.lastIndexOf('/') + 1;
  const podName = location.pathname.substring(podNamePathIndex);

  const { data: pod, error: podError } = usePod(podName);
  const { data: flights, error: flightsError } = useFlights(podName);
  const { data: observations, error: observationsError } = useObservations(podName);

  const [gridProps, setGridProps] = useState<GridProps>();

  useEffect(() => {
    if (observations && observations.length) {
      const cols = [];
      const timeCol = { title: 'time', width: 180 };
      cols.push(timeCol);
      const firstObservation = observations[0];

      const measurementsKeys = firstObservation.measurements
        ? Object.keys(firstObservation.measurements)
        : [];
      const categoriesKeys = firstObservation.categories
        ? Object.keys(firstObservation.categories)
        : [];
      const colWidth =
        (gridWidth - timeCol.width - 17) / (measurementsKeys.length + categoriesKeys.length + 1);

      for (const m of measurementsKeys) {
        cols.push({ title: m, width: colWidth });
      }

      for (const c of categoriesKeys) {
        cols.push({ title: c, width: colWidth });
      }

      cols.push({ title: 'tags', width: colWidth });

      const getGridDataFunc = ([col, row]: readonly [number, number]): GridCell => {
        if (row >= observations.length) {
          return {
            kind: GridCellKind.Number,
            data: undefined,
            displayData: '',
            allowOverlay: false,
          };
        }
        const observation = observations[observations.length - row - 1];
        if (col === 0) {
          return {
            kind: GridCellKind.Number,
            data: observation.time,
            displayData: new Date(observation.time * 1000).toLocaleString(),
            allowOverlay: false,
          };
        }

        if (col >= 1 && col <= measurementsKeys.length) {
          const measurement = observation.measurements[measurementsKeys[col - 1]];
          return {
            kind: GridCellKind.Number,
            data: measurement,
            displayData: measurement.toString(),
            allowOverlay: false,
          };
        }

        if (
          col > measurementsKeys.length &&
          col <= measurementsKeys.length + categoriesKeys.length
        ) {
          const category =
            observation.categories[categoriesKeys[col - measurementsKeys.length - 1]];
          return {
            kind: GridCellKind.Text,
            data: category,
            displayData: category,
            allowOverlay: false,
          };
        }

        if (col == measurementsKeys.length + categoriesKeys.length + 1) {
          const tags = observation.tags ? observation.tags.join(' ') : '';
          return {
            kind: GridCellKind.Text,
            data: tags,
            displayData: tags,
            allowOverlay: false,
          };
        }

        return {
          kind: GridCellKind.Number,
          data: row,
          displayData: row.toString(),
          allowOverlay: false,
        };
      };

      setGridProps({
        columns: cols,
        gridDataFunc: getGridDataFunc,
      });
    }
  }, [observations]);

  return (
    <div className="flex flex-col flex-grow">
      {!podError && pod && (
        <div className="mb-2">
          <PodHeader pod={pod}></PodHeader>
          <h2 className="ml-2 mb-2 font-spice tracking-spice text-s uppercase">Observations</h2>
          {observationsError && (
            <span>An error occurred fetching observations: {observationsError}</span>
          )}
          <div className="border-1 border-gray-300">
            {!observationsError && observations && gridProps && (
              <DataEditorContainer width={gridWidth} height={gridHeight}>
                <DataEditor
                  getCellContent={gridProps.gridDataFunc}
                  columns={gridProps.columns}
                  rows={observations.length}
                  rowMarkers={false}
                />
              </DataEditorContainer>
            )}
          </div>
          <h2 className="mt-4 ml-2 mb-2 font-spice tracking-spice text-s uppercase">
            Training Runs
          </h2>
          <div className="p-2">
            {!flightsError &&
              flights.map((flight, i) => (
                <div key={i}>
                  <Card>
                    <FlightChart flight={flight} />
                  </Card>
                </div>
              ))}
            {(!flights || flights.length === 0) && <span>Pod has no training runs.</span>}
          </div>
        </div>
      )}
    </div>
  );
};

export default PodPage;
