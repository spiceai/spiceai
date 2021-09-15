import React, { useRef, useEffect } from 'react';
import { ChartConfiguration, ChartItem } from 'chart.js';
import Chart from 'chart.js/auto';

import { Flight } from '../../models/flight';

interface FlightChartProps {
  flight: Flight;
}

const colors = [
  "#022132",
  "#1c3155",
  "#4e3d71",
  "#88417e",
  "#bf4578",
  "#ea5562",
  "#ff7840",
  "#ffa600",
]

const FlightChart: React.FunctionComponent<FlightChartProps> = (props) => {
  const chartRef = useRef() as React.MutableRefObject<HTMLCanvasElement>;
  const chart = useRef<Chart<'line'>>();

  useEffect(() => {
    if (chartRef.current) {
      const labels = props.flight.episodes.map((ep) => {
        return `Ep ${ep.episode || 0}`;
      });
      const chartCanvas = chartRef.current as ChartItem;
      const chartConfig: ChartConfiguration<'line'> = {
        type: 'line',
        options: {
          responsive: true,
          animation: false,
          interaction: {
            mode: 'index',
            intersect: false,
          },
          stacked: false,
          scales: {
            x: {
              title: {
                display: true,
                text: 'Episode',
              },
              type: 'category',
            },
            y1: {
              id: "rewards",
              display: false,
              title: {
                display: true,
                text: 'Rewards',
              },
              position: "left",
              type: 'logarithmic',
              weight: 100,
              grid: {
                display: false,
              },
              ticks: {
                display: false,
              }
            },
            y2: {
              id: "actions",
              title: {
                display: true,
                text: "Action Count",
              },
              type: "logarithmic",
              position: "right",
              grid: {
                display: false,
              },
              ticks: {
                display: false,
              }
            }
          },
          plugins: {
            title: {
              display: true,
              text: `Training Run ${props.flight.start.toLocaleString()}`,
            },
            legend: {
              display: true,
            },
          },
        } as any,
        data: {
          labels: labels,
          datasets: [
            {
              label: 'rewards',
              data: props.flight.episodes as any,
              backgroundColor: 'rgba(00, 22, 132, 0.5)',
              borderColor: 'rgba(00, 22, 132, 1)',
              borderWidth: 2,
              pointRadius: 4,
              parsing: {
                xAxisKey: 'episode',
                yAxisKey: 'reward',
              },
              yAxisID: 'rewards',
              normalized: true,
            }
          ],
        },
      };

      if (chartConfig.data.datasets.length == 0 && props.flight.episodes.length) {
        const ep = props.flight.episodes[0]
        let numActions = 0
        for (const actionName of Object.keys(ep.actions_taken)) {
          if (numActions >= colors.length) {
            // Only show the first colors.length actions. TODO: Support action selector/filter
            break
          }
          const dataset = {
            label: actionName,
            data: props.flight.episodes as any,
            backgroundColor: colors[numActions] + "22",
            borderColor: colors[numActions] + "DD",
            borderWidth: 1,
            pointRadius: 2,
            parsing: {
              xAxisKey: 'episode',
              yAxisKey: `actions_taken.${actionName}`,
            },
            yAxisID: 'actions',
            normalized: true,
          }
          chartConfig.data.datasets.push(dataset)
          numActions++
        }
      }

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

export default FlightChart;
