import React from 'react';

interface CardProps {
  color?: string;
}

const Card: React.FunctionComponent<CardProps> = (props) => {
  const color = props.color || 'white';

  return (
    <div className={`min-h-24 mb-4 rounded shadow text-primary-dark overflow-hidden bg-${color}`}>
      {props.children}
    </div>
  );
};

export default Card;
