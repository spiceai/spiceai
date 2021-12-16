import React, { useRef, useEffect } from 'react'
import { ChartConfiguration, ChartItem } from 'chart.js'
import Chart from 'chart.js/auto'

import { Pod } from '../../models/pod'
import { Flight } from '../../models/flight'
import TrainingLogger from './TrainingLogger'

interface FlightChartProps {
  pod: Pod
  flight: Flight
}

const colors = [
  '#e6194B',
  '#f58231',
  '#bfef45',
  '#3cb44b',
  '#42d4f4',
  '#9A6324',
  '#000000',
  '#ffe119',
]

const FlightChart: React.FunctionComponent<FlightChartProps> = (props) => {
  const chartRef = useRef() as React.MutableRefObject<HTMLCanvasElement>
  const chart = useRef<Chart<'line'>>()

  useEffect(() => {
    if (chartRef.current) {
      const labels = props.flight.episodes.map((ep) => {
        return `Ep ${ep.episode || 0}`
      })
      const chartCanvas = chartRef.current as ChartItem
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
              display: true,
              title: {
                display: true,
                text: 'Rewards',
              },
              position: 'left',
              type: 'linear',
              weight: 100,
              grid: {
                display: true,
              },
            },
            y2: {
              display: true,
              title: {
                display: true,
                text: 'Action Count',
              },
              type: 'linear',
              position: 'right',
              grid: {
                display: false,
              },
            },
          },
          plugins: {
            title: {
              display: true,
              text: `Training Run ${props.flight.start.toLocaleString()}`,
            },
            subtitle: {
              display: true,
              text: `Episodes: ${props.flight.episodes.length}/${props.pod.episodes}  |  Learning algorithm: ${props.flight.algorithm}`,
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
              borderWidth: 3,
              pointRadius: 5,
              parsing: {
                xAxisKey: 'episode',
                yAxisKey: 'reward',
              },
              yAxisID: 'y1',
            },
          ],
        },
      }

      if (chartConfig.data.datasets.length == 1 && props.flight?.episodes.length) {
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
            backgroundColor: colors[numActions] + '22',
            borderColor: colors[numActions] + 'DD',
            borderWidth: 2,
            pointRadius: 3,
            parsing: {
              xAxisKey: 'episode',
              yAxisKey: `actions_taken.${actionName}`,
            },
            yAxisID: 'y2',
          }
          chartConfig.data.datasets.push(dataset)
          numActions++
        }
      }

      if (chart.current) {
        chart.current.options = chartConfig.options as any
        chart.current.data = chartConfig.data
        chart.current.update()
      } else {
        chart.current = new Chart(chartCanvas, chartConfig)
      }
    }
  }, [chartRef, props.flight])

  return (
    <div className="rounded relative">
      <div className="flex">
        <div className="flex flex-grow"></div>
        <div className="mt-1 mr-2">
          {props.flight.loggers &&
            props.flight.loggers.map((logger) => {
              return <TrainingLogger id={logger} />
            })}
        </div>
      </div>
      <canvas className="p-2" ref={chartRef}></canvas>
    </div>
  )
}

export default FlightChart
