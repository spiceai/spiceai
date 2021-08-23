import React from 'react';

const ContentPanel: React.FunctionComponent = (props) => {
  const { children } = props;
  return (
    <div className="flex flex-col flex-grow bg-gradient-to-b from-white to-cloud p-4">
      <div>{children}</div>
      <div className="flex-grow" />
    </div>
  );
};

export default ContentPanel;
