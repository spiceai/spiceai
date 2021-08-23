import React, { useRef, useEffect } from 'react';
import { ChartConfiguration, ChartItem } from 'chart.js';
import Chart from 'chart.js/auto';

import { Flight } from '../../models/flight'

interface FlightChartProps {
  flight: Flight
}

const FlightChart: React.FunctionComponent<FlightChartProps> = (props) => {

  const chartRef = useRef() as React.MutableRefObject<HTMLCanvasElement>;
  const chart = useRef<Chart<'line'>>();

  useEffect(() => {
    if (chartRef.current) {
      const labels = props.flight.episodes.map(ep => {
        return `Ep ${ep.episode}`
      })
      const chartCanvas = chartRef.current as ChartItem;
      const chartConfig: ChartConfiguration<'line'> = {
        type: 'line',
        options: {
          responsive: true,
          animation: false,
          scales: {
            x: {
              title: {
                display: true,
                text: "Episode"
              },
              type: 'category',
            },
            y1: {
              title: {
                display: true,
                text: "Rewards"
              },
              type: 'linear',
              grace: "10%",
            }
          },
          plugins: {
            title: {
              display: true,
              text: `Flight ${props.flight.start.toLocaleString()}`
            },
            legend: {
              display: true,
            },
            boxselect: {
              select: {
                enabled: true,
                direction: 'x',
              },
            },
          },
        } as any,
        data: {
          labels: labels,
          datasets: [
            {
              label: "rewards",
              data: props.flight.episodes as any,
              borderColor: 'rgba(00, 22, 132, 1)',
              borderWidth: 2,
              pointRadius: 5,
              parsing: {
                xAxisKey: 'episode',
                yAxisKey: 'reward',
              },
            },
          ],
        },
      };

      if (chart.current) {
        chart.current.options = chartConfig.options as any;
        chart.current.data = chartConfig.data;
        chart.current.update();
      } else {
        chart.current = new Chart(chartCanvas, chartConfig);
      }
    }
  }, [chartRef, props.flight]);

  return (
    <div className="rounded relative">
      <canvas className="p-2" ref={chartRef}></canvas>
    </div>
  );
};

export default FlightChart