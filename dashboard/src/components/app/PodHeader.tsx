import React from 'react';
import { Pod } from '../../models/pod'

interface PodHeaderProps {
  pod: Pod;
}

const PodHeader: React.FunctionComponent<PodHeaderProps> = (props) => (
  <React.Fragment>
    <div className="m-4 flex gap-4">
      <div>
        <div className="bg-red-500 rounded-full h-16 w-16 flex items-center justify-center text-2xl text-semibold uppercase">
          {props.pod.name.substr(0, 1)}
        </div>
      </div>
      <div>
        <h1 className="text-2xl text-blue-600 text-semibold">{props.pod.name}</h1>
        <span className="text-sm text-gray-500">{props.pod.manifest_path}</span>
      </div>
    </div>
  </React.Fragment>
);

export default PodHeader;
