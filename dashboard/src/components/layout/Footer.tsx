import React from 'react';

const Footer: React.FunctionComponent = () => {
  return (
    <footer className="flex flex-col text-sm text-footer m-2 bg-primary-dark text-right">
      <div>
        <span className="text-xs"><a href="https://github.com/spiceai/spiceai">github.com/spiceai/spiceai</a></span>
      </div>
    </footer>
  );
};

export default Footer;
