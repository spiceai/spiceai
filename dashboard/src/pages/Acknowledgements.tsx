import React, { useState, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import acknowledgements from '../content/acknowledgements.md';

const Acknowledgements: React.FunctionComponent = () => {
  const [acknowledgementsMarkdown, setAcknowledgementsMarkdown] = useState('');

  useEffect(() => {
    const fetchContent = async () => {
      const response = await fetch(acknowledgements);
      const text = await response.text();
      setAcknowledgementsMarkdown(text);
    };
    fetchContent();
  }, []);

  return (
    <section className="p-8">
      <ReactMarkdown className="markdown">{acknowledgementsMarkdown}</ReactMarkdown>
    </section>
  );
};

export default Acknowledgements;