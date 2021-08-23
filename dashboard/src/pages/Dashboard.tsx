import React from 'react';
import { usePods } from '../models/pod';
import PodCard from '../components/layout/PodCard'

const Dashboard: React.FunctionComponent = () => {

  const { data: pods, error: podsError, isLoading: podsLoading } = usePods();

  return (
    <>
      <h2 className="mb-2 font-spice tracking-spice text-s uppercase">Pods</h2>
      { podsError &&
        <p>An error occurred while getting pods: { podsError }</p>
      }
      { !podsError && pods.map((pod, i) => (
        <div key={i}>
          <PodCard
            isLoading={podsLoading}
            podName={pod.name}
            fqid={pod.manifest_path}
            route={`/pods/${pod.name}`}
          >
          </PodCard>
        </div>
      ))}
    </>
  );
};

export default Dashboard;
