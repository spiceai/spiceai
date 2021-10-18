import React from 'react';

interface GridItemProps {
  heading?: boolean;
  first?: boolean;
}

const GridItem: React.FunctionComponent<GridItemProps> = (props) => {
  return (
    <div
      className={`${props.heading && 'bg-gray-200'} ${
        props.first && 'border-l-0'
      } p-2 border border-gray-300 border-t-0 border-r-0 w-full text-center`}
    >
      {props.children}
    </div>
  );
};

export default GridItem;
